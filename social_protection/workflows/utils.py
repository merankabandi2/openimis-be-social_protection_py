"""
Functionalities shared between different python workflows.

"""
import json
import logging
from abc import ABCMeta, abstractmethod
from typing import Iterable

from django.db import ProgrammingError, connection

from core.models import User
from individual.models import IndividualDataSource
from social_protection.apps import SocialProtectionConfig
from social_protection.models import BenefitPlan
from social_protection.services import BeneficiaryImportService
from social_protection.utils import load_dataframe
from workflow.exceptions import PythonWorkflowHandlerException

logger = logging.getLogger(__name__)


class BasePythonWorkflowExecutor(metaclass=ABCMeta):

    def __init__(self, benefit_plan_uuid, upload_uuid, user_uuid, accepted=None):
        self.benefit_plan_uuid = benefit_plan_uuid
        self.upload_uuid = upload_uuid
        self.user_uuid = user_uuid
        self.user = User.objects.get(id=self.user_uuid)
        self.accepted = accepted
        self._load_df()

    def _load_df(self):
        benefit_plan = BenefitPlan.objects.filter(uuid=self.benefit_plan_uuid, is_deleted=False).first()
        df = load_dataframe(IndividualDataSource.objects.filter(upload_id=self.upload_uuid))
        self.benefit_plan = benefit_plan
        self.df = self.clean_data(df)
        self.schema = benefit_plan.beneficiary_data_schema

    @staticmethod
    def clean_data(df):
        if 'Unnamed: 0' in df.columns:
            # Drop the 'Unnamed: 0' column
            df.drop('Unnamed: 0', axis=1, inplace=True)
            logger.info("Provided dataframe contains Unnamed column for python workflow. "
                        "It'll be removed from upload.")
        return df

    def validate_dataframe_headers(self, is_update=False):
        """
        Validates if DataFrame headers:
        1. Are included in the JSON schema properties.
        2. Include 'first_name', 'last_name', and 'dob'.
        3. 'id' is field automatically added to DataFrame which is used for upload.
        4. If action is data upload then 'ID' unique identifier is required as well.
        """
        
        df_headers = set(self.df.columns)
        schema = json.loads(self.schema) if isinstance(self.schema, str) else self.schema
        schema_properties = set(schema.get('properties', {}).keys())
        schema_properties.update(['recipient_info', 'group_code', 'individual_role'])
        required_headers = set(SocialProtectionConfig.beneficiary_base_fields)
        
        if is_update:
            required_headers.add('ID')

        errors = []
        if not (df_headers - required_headers).issubset(schema_properties):
            invalid_headers = df_headers - schema_properties - required_headers
            errors.append(
                F"Uploaded beneficiaries contains invalid columns: {invalid_headers}"
            )

        for field in required_headers:
            if field not in df_headers:
                errors.append(
                    F"Uploaded beneficiaries missing essential header: {field}"
                )

        if errors:
            raise PythonWorkflowHandlerException("\n".join(errors))

    @abstractmethod
    def execute(self, **kwargs):
        pass


class SqlProcedurePythonWorkflow(BasePythonWorkflowExecutor):
    """
        Implementation of the PythonWorkflowExecutor that executes provided sql with
            current_upload_id, userUUID, benefitPlanUUID
        parameters.
    """

    def execute(self, sql: str, params: Iterable):
        try:
            self._execute_sql_logic(sql, params)
        except ProgrammingError as e:
            # The exception on procedure execution is handled by the procedure itself.
            logger.log(logging.WARNING, F'Error during beneficiary upload workflow, details:\n{str(e)}')
            return
        except Exception as e:
            raise PythonWorkflowHandlerException(str(e))

    def _execute_sql_logic(self, sql_func: str, params: Iterable):
        with connection.cursor() as cursor:
            current_upload_id = self.upload_uuid
            userUUID = self.user_uuid
            benefitPlan = self.benefit_plan_uuid
            accepted = self.accepted
            # The SQL logic here needs to be carefully translated or executed directly
            # The provided SQL is complex and may require breaking down into multiple steps or ORM operations
            cursor.execute(
                sql_func, params #[current_upload_id, userUUID, benefitPlan, accepted]
            )
            # Process the cursor results or handle exceptions


class MakerCheckerPythonWorkflowExecutor(SqlProcedurePythonWorkflow, metaclass=ABCMeta):
    """
    Implementation of the PythonWorkflowExecutor that is relying on the maker-checker logic.
    If the maker-checker logic is not applied then it's executing provided sql with
        current_upload_id, userUUID, benefitPlanUUID
    parameters.

    If the uploaded dataset is invalid in terms of the calculation rules validation, then new task is created.
    New task is also created in case maker-checker logic is enabled in the config.
    """
    @property
    def should_create_task(self) -> bool:
        """
        Property saying whether the maker-checker logic is enabled for given entity.
        """
        raise NotImplementedError()

    @abstractmethod
    def _create_task_function(self):
        """
        Function responsible for creating new task.
        """
        raise NotImplementedError()

    def execute(self, sql):
        try:
            if self.should_create_task:
                # If some records were not validated, call the task creation service
                self._create_task_function()
            else:
                # All records are fine, execute SQL logic
                self._execute_sql_logic(sql)
        except ProgrammingError as e:
            import traceback
            # The exception on procedure execution is handled by the procedure itself.
            logger.log(logging.ERROR, F'Error during beneficiary upload workflow, details:\n{str(e)}')
            return
        except Exception as e:
            import traceback
            logger.log(logging.ERROR, F'Unexpected during beneficiary upload workflow, details:\n{str(e)}')
            raise PythonWorkflowHandlerException(str(e))


class DataUploadWorkflow(MakerCheckerPythonWorkflowExecutor):

    def __init__(self, benefit_plan_uuid, upload_uuid, user_uuid, import_service=BeneficiaryImportService):
        super().__init__(benefit_plan_uuid, upload_uuid, user_uuid)
        self.import_service = import_service(self.user)

    @property
    def should_create_task(self):
        validation_response = self.import_service.validate_import_beneficiaries(
            upload_id=self.upload_uuid,
            individual_sources=IndividualDataSource.objects.filter(upload_id=self.upload_uuid),
            benefit_plan=self.benefit_plan
        )
        return validation_response['summary_invalid_items'] or True  # Replace this with config check

    def _create_task_function(self):
        self.import_service.create_task_with_importing_valid_items(self.upload_uuid, self.benefit_plan)


class DataUpdateWorkflow(MakerCheckerPythonWorkflowExecutor):

    def __init__(self, benefit_plan_uuid, upload_uuid, user_uuid, import_service=BeneficiaryImportService):
        super().__init__(benefit_plan_uuid, upload_uuid, user_uuid)
        self.import_service = import_service(self.user)

    @property
    def should_create_task(self):
        validation_response = self.import_service.validate_import_beneficiaries(
            upload_id=self.upload_uuid,
            individual_sources=IndividualDataSource.objects.filter(upload_id=self.upload_uuid),
            benefit_plan=self.benefit_plan
        )
        return validation_response['summary_invalid_items'] or True  # Replace this with config check

    def _create_task_function(self):
        self.import_service.create_task_with_update_valid_items(self.upload_uuid, self.benefit_plan)
