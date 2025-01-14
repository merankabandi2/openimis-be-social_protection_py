import logging
import uuid

from django.core.exceptions import ValidationError
from individual.models import (
    IndividualDataSourceUpload,
    IndividualDataSource,
    GroupIndividual
)
from social_protection.apps import SocialProtectionConfig
from social_protection.models import (
    BenefitPlanDataUploadRecords,
    BenefitPlan,
    GroupBeneficiary
)
from social_protection.utils import calculate_percentage_of_invalid_items
from tasks_management.models import Task
from tasks_management.apps import TasksManagementConfig
from tasks_management.services import (
    UpdateCheckerLogicServiceMixin,
    CreateCheckerLogicServiceMixin,
    crud_business_data_builder,
    TaskService,
    _get_std_task_data_payload
)

logger = logging.getLogger(__name__)


def on_confirm_enrollment_of_group(**kwargs):
    from core import datetime
    result = kwargs.get('result', None)
    benefit_plan_id = result['benefit_plan_id']
    status = result['status']
    user = result['user']
    groups_to_upload = result['groups_not_assigned_to_selected_programme']
    group_ids = groups_to_upload.values_list('id', flat=True)
    if SocialProtectionConfig.enable_maker_checker_logic_enrollment:
        benefit_plan = BenefitPlan.objects.get(id=benefit_plan_id)
        upload = IndividualDataSourceUpload(
            source_name=f"Enrollment into {benefit_plan.code} {datetime.date.today()}",
            source_type='beneficiary group import'
        )
        upload.save(username=user.login_name)
        upload_record = BenefitPlanDataUploadRecords(
            data_upload=upload,
            benefit_plan_id=benefit_plan_id,
            workflow="Enrollment"
        )
        upload_record.save(username=user.username)

        group_individuals = GroupIndividual.objects.filter(
            is_deleted=False,
            group_id__in=group_ids,
            role=GroupIndividual.Role.HEAD
        ).distinct()
        data_source_objects = []
        for group_individual in group_individuals:
            source = IndividualDataSource(
                upload=upload,
                individual=group_individual.individual,
                json_ext=group_individual.group.json_ext,
                validations={},
                user_created=user,
                user_updated=user,
                uuid=uuid.uuid4(),
            )
            data_source_objects.append(source)
        IndividualDataSource.objects.bulk_create(data_source_objects)
        json_ext = {
            'source_name': upload_record.data_upload.source_name,
            'workflow': upload_record.workflow,
            'percentage_of_invalid_items': calculate_percentage_of_invalid_items(upload_record.id),
            'data_upload_id': str(upload.id),
            'benefit_plan_id': benefit_plan_id,
            'beneficiary_status': status
        }
        TaskService(user).create({
            'source': 'import_valid_items',
            'entity': upload_record,
            'status': Task.Status.RECEIVED,
            'executor_action_event': TasksManagementConfig.default_executor_event,
            'business_event': SocialProtectionConfig.validation_group_enrollment,
            'json_ext': json_ext
        })
    else:
        for group in groups_to_upload:
            head_group_individual = GroupIndividual.objects.filter(
                is_deleted=False,
                group=group,
                role=GroupIndividual.Role.HEAD
            ).first()
            group_beneficiary = GroupBeneficiary(
                group=group,
                benefit_plan_id=benefit_plan_id,
                status=status,
                json_ext=group.json_ext
            )
            try:
                group_beneficiary.save(username=user.username)
            except ValidationError as e:
                logger.error(f"Validation error occurred: {e}")
