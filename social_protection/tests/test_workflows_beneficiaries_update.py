from django.db import connection
from django.test import TestCase
from core.test_helpers import create_test_interactive_user
from individual.models import (
    Individual,
    IndividualDataSource,
    IndividualDataSourceUpload,
)
from social_protection.models import BenefitPlanDataUploadRecords, Beneficiary, BeneficiaryStatus
from social_protection.services import BeneficiaryService
from social_protection.workflows.base_beneficiary_update import process_update_beneficiaries_workflow
from social_protection.tests.test_helpers import add_individual_to_benefit_plan, create_benefit_plan, create_individual
from location.test_helpers import create_test_village
from unittest.mock import patch
import uuid
from unittest import skipIf


@skipIf(
    connection.vendor != "postgresql",
    "Skipping tests due to implementation usage of validate_json_schema, which is a postgres specific extension."
)
class ProcessUpdateBeneficiariesWorkflowTest(TestCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        # Patch validate_dataframe_headers as it is already tested separately
        cls.validate_headers_patcher = patch(
            "social_protection.workflows.utils.BasePythonWorkflowExecutor.validate_dataframe_headers",
            lambda self, is_update: None
        )
        cls.validate_headers_patcher.start()

        cls.schema_patcher = patch("individual.apps.IndividualConfig.individual_schema", "{}")
        cls.schema_patcher.start()

    @classmethod
    def tearDownClass(cls):
        cls.validate_headers_patcher.stop()
        super().tearDownClass()

    def setUp(self):
        self.user = create_test_interactive_user(username="admin")
        self.user_uuid = self.user.id

        self.plan_schema = {"properties": {}}
        self.benefit_plan = create_benefit_plan(self.user.username, {
                "name": "Test Benefit Plan",
                "description": "A test benefit plan",
                "code": "TESTPlan",
                "max_beneficiaries": 1000,
                "beneficiary_data_schema": {}
            })

        individual1_dict = {
            'first_name': 'Foo 1',
            'last_name': 'Bar',
            "dob": "1998-01-01",
            'json_ext': {},
        }
        self.individual1 = create_individual(self.user.username, individual1_dict)

        individual2_dict = {
            'first_name': 'Foo 2',
            'last_name': 'Baz',
            "dob": "1980-01-01",
            'json_ext': {},
        }
        self.individual2 = create_individual(self.user.username, individual2_dict)
        
        self.service = BeneficiaryService(self.user)
        
        # Add individuals to the benefit plan and create beneficiaries
        self.beneficiary1_uuid = add_individual_to_benefit_plan(
            self.service, 
            self.individual1, 
            self.benefit_plan, 
            payload_override={'status': 'ACTIVE'}
        )
        self.beneficiary2_uuid = add_individual_to_benefit_plan(
            self.service, 
            self.individual2, 
            self.benefit_plan, 
            payload_override={'status': 'ACTIVE'}
        )

        self.upload = IndividualDataSourceUpload(
            source_name='csv',
            source_type='update',
            status="PENDING",
        )
        self.upload.save(user=self.user)
        self.upload_uuid = self.upload.id

        upload_record = BenefitPlanDataUploadRecords(
            data_upload=self.upload,
            workflow='Python Beneficiaries Update',
            benefit_plan=self.benefit_plan,
            json_ext={}
        )
        upload_record.save(user=self.user.user)

        self.village = create_test_village({
            'name': 'McLean',
            'code': 'VwA',
        })
        self.individual1_updated_first_name = "John"
        self.beneficiary1_updated_status = BeneficiaryStatus.POTENTIAL
        self.valid_data_source = IndividualDataSource(
            upload_id=self.upload_uuid,
            json_ext={
                "ID": str(self.beneficiary1_uuid),
                "first_name": self.individual1_updated_first_name,
                "last_name": "Doe",
                "dob": "1980-01-01",
                "status": self.beneficiary1_updated_status,
                "location_name": self.village.name,
                "location_code": self.village.code,
            }
        )
        self.valid_data_source.save(user=self.user)

        self.individual2_updated_first_name = "Jane"
        self.invalid_data_source = IndividualDataSource(
            upload_id=self.upload_uuid,
            json_ext={
                "ID": str(uuid.uuid4()),
                "first_name": self.individual2_updated_first_name,
            }
        )
        self.invalid_data_source.save(user=self.user)

    @patch('individual.apps.IndividualConfig.enable_maker_checker_for_individual_update', False)
    @patch('social_protection.apps.SocialProtectionConfig.enable_maker_checker_for_beneficiary_update', False)
    @skipIf(
        connection.vendor != "postgresql",
        "Skipping tests due to implementation usage of validate_json_schema, which is a postgres specific extension."
    )
    def test_process_update_beneficiaries_workflow_successful_execution(self):
        process_update_beneficiaries_workflow(self.user_uuid, self.benefit_plan.uuid, self.upload_uuid)

        upload = IndividualDataSourceUpload.objects.get(id=self.upload_uuid)

        # Check that the status is 'FAIL' due to the entry with invalid ID
        self.assertEqual(upload.status, "FAIL")
        self.assertIsNotNone(upload.error)
        errors = upload.error['errors']
        self.assertIn("Invalid entries", errors['error'])

        # Check that the correct failing entries are logged in the error field
        error_key = "failing_entries_invalid_id"
        self.assertIn(error_key, errors)
        self.assertIn(str(self.invalid_data_source.id), errors[error_key]['uuids'])
        self.assertNotIn(str(self.valid_data_source.id), errors[error_key]['uuids'])

        # individual_id should not be assigned for any data sources
        data_entries = IndividualDataSource.objects.filter(upload_id=self.upload_uuid)
        for entry in data_entries:
            self.assertIsNone(entry.individual_id)

        # individual data should not be updated
        individual1_from_db = Individual.objects.get(id=self.individual1.id)
        self.assertNotEqual(individual1_from_db.first_name, self.individual1_updated_first_name)
        
        beneficiary1_from_db = Beneficiary.objects.get(id=self.beneficiary1_uuid)
        self.assertNotEqual(beneficiary1_from_db.status, self.beneficiary1_updated_status)

        individual2_from_db = Individual.objects.get(id=self.individual2.id)
        self.assertNotEqual(individual2_from_db.first_name, self.individual2_updated_first_name)

    @patch('individual.apps.IndividualConfig.enable_maker_checker_for_individual_update', False)
    @patch('social_protection.apps.SocialProtectionConfig.enable_maker_checker_for_beneficiary_update', False)
    @skipIf(
        connection.vendor != "postgresql",
        "Skipping tests due to implementation usage of validate_json_schema, which is a postgres specific extension."
    )
    def test_process_update_beneficiaries_workflow_with_all_valid_entries(self):
        # Update invalid entry in IndividualDataSource to valid data
        self.invalid_data_source.json_ext = {
            "ID": str(self.beneficiary2_uuid),
            "first_name": self.individual2_updated_first_name,
            "location_name": None,
            "location_code": None,
        }
        self.invalid_data_source.save(user=self.user)

        process_update_beneficiaries_workflow(self.user_uuid, self.benefit_plan.uuid, self.upload_uuid)

        upload = IndividualDataSourceUpload.objects.get(id=self.upload_uuid)

        self.assertEqual(upload.status, "SUCCESS", upload.error)
        self.assertEqual(upload.error, {})

        # Verify that individual IDs have been assigned to data entries in IndividualDataSource
        data_entries = IndividualDataSource.objects.filter(upload_id=self.upload_uuid)
        for entry in data_entries:
            self.assertIsNotNone(entry.individual_id)

        # individual data should be updated
        individual1_from_db = Individual.objects.get(id=self.individual1.id)
        self.assertEqual(individual1_from_db.first_name, self.individual1_updated_first_name)
        self.assertEqual(individual1_from_db.location.name, self.village.name)
        
        # Beneficiary status update shouldn't make any effect as it is not supported
        beneficiary1_from_db = Beneficiary.objects.get(id=self.beneficiary1_uuid)
        self.assertNotEqual(beneficiary1_from_db.status, self.beneficiary1_updated_status)

        individual2_from_db = Individual.objects.get(id=self.individual2.id)
        self.assertEqual(individual2_from_db.first_name, self.individual2_updated_first_name)
        self.assertIsNone(individual2_from_db.location)

    @patch('individual.apps.IndividualConfig.enable_maker_checker_for_individual_update', True)
    @patch('social_protection.apps.SocialProtectionConfig.enable_maker_checker_for_beneficiary_update', True)
    def test_process_update_beneficiaries_workflow_with_maker_checker_enabled(self):
        # Update invalid entry in IndividualDataSource to valid data
        self.invalid_data_source.json_ext = {
            "ID": str(self.beneficiary2_uuid),
            "first_name": "Jane",
            "location_name": None,
            "location_code": None,
        }
        self.invalid_data_source.save(user=self.user)

        process_update_beneficiaries_workflow(self.user_uuid, self.benefit_plan.uuid, self.upload_uuid)

        upload = IndividualDataSourceUpload.objects.get(id=self.upload_uuid)

        self.assertEqual(upload.status, "WAITING_FOR_VERIFICATION")
        self.assertEqual(upload.error, {})

        # Verify that individual IDs not yet assigned to data entries in IndividualDataSource
        data_entries = IndividualDataSource.objects.filter(upload_id=self.upload_uuid)
        for entry in data_entries:
            self.assertIsNone(entry.individual_id)

        # individual data should not be updated
        individual1_from_db = Individual.objects.get(id=self.individual1.id)
        self.assertNotEqual(individual1_from_db.first_name, self.individual1_updated_first_name)

        individual2_from_db = Individual.objects.get(id=self.individual2.id)
        self.assertNotEqual(individual2_from_db.first_name, self.individual2_updated_first_name)
