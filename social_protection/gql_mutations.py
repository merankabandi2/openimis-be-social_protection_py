import graphene as graphene
from django.contrib.auth.models import AnonymousUser
from django.core.exceptions import ValidationError
from django.db import transaction
from datetime import datetime

from core.gql.gql_mutations.base_mutation import BaseHistoryModelCreateMutationMixin, BaseMutation, \
    BaseHistoryModelUpdateMutationMixin, BaseHistoryModelDeleteMutationMixin
from core.schema import OpenIMISMutation
from social_protection.apps import SocialProtectionConfig
from social_protection.models import (
    BenefitPlan,
    Beneficiary, GroupBeneficiary, BeneficiaryStatus, BenefitPlanMutation, LocationBenefitPlanPaymentPoint
)
from social_protection.services import (
    BenefitPlanService,
    BeneficiaryService, GroupBeneficiaryService, LocationBenefitPlanPaymentPointService
)


# Define the enum once to avoid naming conflicts
class BeneficiaryStatusEnum(graphene.Enum):
    POTENTIAL = BeneficiaryStatus.POTENTIAL
    VALIDATED = BeneficiaryStatus.VALIDATED
    ACTIVE = BeneficiaryStatus.ACTIVE
    GRADUATED = BeneficiaryStatus.GRADUATED
    SUSPENDED = BeneficiaryStatus.SUSPENDED


def check_perms_for_field(user, permission, data, field_string):
    if data.get(field_string, None) and not user.has_perms(permission):
        raise ValidationError("mutation.lack_of_schema_perms")


class CreateBenefitPlanInputType(OpenIMISMutation.Input):
    class BenefitPlanTypeEnum(graphene.Enum):
        INDIVIDUAL = BenefitPlan.BenefitPlanType.INDIVIDUAL_TYPE
        GROUP = BenefitPlan.BenefitPlanType.GROUP_TYPE

    code = graphene.String(required=True)
    name = graphene.String(required=True, max_length=255)
    max_beneficiaries = graphene.Int(default_value=None)
    ceiling_per_beneficiary = graphene.Decimal(max_digits=18, decimal_places=2, required=False)
    institution = graphene.String(required=False, max_length=255)
    beneficiary_data_schema = graphene.types.json.JSONString(required=False)
    type = graphene.Field(BenefitPlanTypeEnum, required=True)

    date_valid_from = graphene.Date(required=True)
    date_valid_to = graphene.Date(required=True)
    json_ext = graphene.types.json.JSONString(required=False)
    description = graphene.String(required=False, max_length=1024)

    def resolve_type(self, info):
        return self.type


class UpdateBenefitPlanInputType(CreateBenefitPlanInputType):
    id = graphene.UUID(required=True)


class CreateGenericBeneficiaryInputType(OpenIMISMutation.Input):
    status = graphene.Field(BeneficiaryStatusEnum, required=True)
    benefit_plan_id = graphene.UUID(required=False)

    date_valid_from = graphene.Date(required=False)
    date_valid_to = graphene.Date(required=False)
    json_ext = graphene.types.json.JSONString(required=False)


class UpdateGenericBeneficiaryInputType(CreateGenericBeneficiaryInputType):
    id = graphene.UUID(required=True)


class CreateBeneficiaryInputType(CreateGenericBeneficiaryInputType):
    individual_id = graphene.UUID(required=False)


class CreateGroupBeneficiaryInputType(CreateGenericBeneficiaryInputType):
    group_id = graphene.UUID(required=False)


class UpdateBeneficiaryInputType(UpdateGenericBeneficiaryInputType):
    pass


class UpdateGroupBeneficiaryInputType(UpdateGenericBeneficiaryInputType):
    pass


class CreateBenefitPlanMutation(BaseHistoryModelCreateMutationMixin, BaseMutation):
    _mutation_class = "CreateBenefitPlanMutation"
    _mutation_module = "social_protection"
    _model = BenefitPlan

    @classmethod
    def _validate_mutation(cls, user, **data):
        if type(user) is AnonymousUser or not user.has_perms(
                SocialProtectionConfig.gql_benefit_plan_create_perms):
            raise ValidationError("mutation.authentication_required")
        check_perms_for_field(
            user, SocialProtectionConfig.gql_schema_create_perms, data, 'beneficiary_data_schema'
        )
        check_perms_for_field(
            user, SocialProtectionConfig.gql_schema_create_perms, data, 'json_ext'
        )

    @classmethod
    def _mutate(cls, user, **data):
        client_mutation_id = data.pop('client_mutation_id', None)
        if "client_mutation_label" in data:
            data.pop('client_mutation_label')

        service = BenefitPlanService(user)
        res = service.create(data)
        if client_mutation_id and res['success']:
            payroll_id = res['data']['id']
            benefit_plan = BenefitPlan.objects.get(id=payroll_id)
            BenefitPlanMutation.object_mutated(
                user, client_mutation_id=client_mutation_id, benefit_plan=benefit_plan
            )
        if not res['success']:
            return res
        return None

    class Input(CreateBenefitPlanInputType):
        pass


class UpdateBenefitPlanMutation(BaseHistoryModelUpdateMutationMixin, BaseMutation):
    _mutation_class = "UpdateBenefitPlanMutation"
    _mutation_module = "social_protection"
    _model = BenefitPlan

    @classmethod
    def _validate_mutation(cls, user, **data):
        super()._validate_mutation(user, **data)
        if type(user) is AnonymousUser or not user.has_perms(
                SocialProtectionConfig.gql_benefit_plan_update_perms):
            raise ValidationError("mutation.authentication_required")
        check_perms_for_field(
            user, SocialProtectionConfig.gql_schema_update_perms, data, 'beneficiary_data_schema'
        )
        check_perms_for_field(
            user, SocialProtectionConfig.gql_schema_update_perms, data, 'json_ext'
        )

    @classmethod
    def _mutate(cls, user, **data):
        if "date_valid_to" not in data:
            data['date_valid_to'] = None
        if "client_mutation_id" in data:
            data.pop('client_mutation_id')
        if "client_mutation_label" in data:
            data.pop('client_mutation_label')

        service = BenefitPlanService(user)
        if SocialProtectionConfig.gql_check_benefit_plan_update:
            if 'max_beneficiaries' not in data:
                data['max_beneficiaries'] = None
            res = service.create_update_task(data)
        else:
            res = service.update(data)

        return res if not res['success'] else None

    class Input(UpdateBenefitPlanInputType):
        pass


class DeleteBenefitPlanMutation(BaseHistoryModelDeleteMutationMixin, BaseMutation):
    _mutation_class = "DeleteBenefitPlanMutation"
    _mutation_module = "social_protection"
    _model = BenefitPlan

    @classmethod
    def _validate_mutation(cls, user, **data):
        if type(user) is AnonymousUser or not user.id or not user.has_perms(
                SocialProtectionConfig.gql_benefit_plan_delete_perms):
            raise ValidationError("mutation.authentication_required")

    @classmethod
    def _mutate(cls, user, **data):
        if "client_mutation_id" in data:
            data.pop('client_mutation_id')
        if "client_mutation_label" in data:
            data.pop('client_mutation_label')

        service = BenefitPlanService(user)
        ids = data.get('ids')
        if not ids:
            return {'success': False, 'message': 'No IDs to delete', 'details': ''}

        with transaction.atomic():
            for obj_id in ids:
                res = service.delete({'id': obj_id, 'user': user})
                if not res['success']:
                    transaction.rollback()
                    return res

    class Input(OpenIMISMutation.Input):
        ids = graphene.List(graphene.UUID)


class CreateBeneficiaryMutation(BaseHistoryModelCreateMutationMixin, BaseMutation):
    _mutation_class = "CreateBeneficiaryMutation"
    _mutation_module = "social_protection"
    _model = Beneficiary

    @classmethod
    def _validate_mutation(cls, user, **data):
        if type(user) is AnonymousUser or not user.has_perms(
                SocialProtectionConfig.gql_beneficiary_create_perms):
            raise ValidationError("mutation.authentication_required")
        check_perms_for_field(
            user, SocialProtectionConfig.gql_schema_create_perms, data, 'json_ext'
        )

    @classmethod
    def _mutate(cls, user, **data):
        if "client_mutation_id" in data:
            data.pop('client_mutation_id')
        if "client_mutation_label" in data:
            data.pop('client_mutation_label')

        service = BeneficiaryService(user)
        if SocialProtectionConfig.gql_check_beneficiary_crud:
            res = service.create_create_task(data)
        else:
            res = service.create(data)

        return res if not res['success'] else None

    class Input(CreateBeneficiaryInputType):
        pass


class UpdateBeneficiaryMutation(BaseHistoryModelUpdateMutationMixin, BaseMutation):
    _mutation_class = "UpdateBeneficiaryMutation"
    _mutation_module = "social_protection"
    _model = Beneficiary

    @classmethod
    def _validate_mutation(cls, user, **data):
        super()._validate_mutation(user, **data)
        if type(user) is AnonymousUser or not user.has_perms(
                SocialProtectionConfig.gql_beneficiary_update_perms):
            raise ValidationError("mutation.authentication_required")
        check_perms_for_field(
            user, SocialProtectionConfig.gql_schema_update_perms, data, 'json_ext'
        )

    @classmethod
    def _mutate(cls, user, **data):
        if "date_valid_to" not in data:
            data['date_valid_to'] = None
        if "client_mutation_id" in data:
            data.pop('client_mutation_id')
        if "client_mutation_label" in data:
            data.pop('client_mutation_label')

        service = BeneficiaryService(user)
        if SocialProtectionConfig.gql_check_beneficiary_crud:
            res = service.create_update_task(data)
        else:
            res = service.update(data)

        return res if not res['success'] else None

    class Input(UpdateBeneficiaryInputType):
        pass


class DeleteBeneficiaryMutation(BaseHistoryModelDeleteMutationMixin, BaseMutation):
    _mutation_class = "DeleteBeneficiaryMutation"
    _mutation_module = "social_protection"
    _model = Beneficiary

    @classmethod
    def _validate_mutation(cls, user, **data):
        if type(user) is AnonymousUser or not user.id or not user.has_perms(
                SocialProtectionConfig.gql_beneficiary_delete_perms):
            raise ValidationError("mutation.authentication_required")

    @classmethod
    def _mutate(cls, user, **data):
        if "client_mutation_id" in data:
            data.pop('client_mutation_id')
        if "client_mutation_label" in data:
            data.pop('client_mutation_label')

        service = BeneficiaryService(user)

        ids = data.get('ids')
        if not ids:
            return {'success': False, 'message': 'No IDs to delete', 'details': ''}

        with transaction.atomic():
            for obj_id in ids:
                if SocialProtectionConfig.gql_check_beneficiary_crud:
                    res = service.create_delete_task({'id': obj_id})
                else:
                    res = service.delete({'id': obj_id, 'user': user})
                if not res['success']:
                    transaction.rollback()
                    return res

    class Input(OpenIMISMutation.Input):
        ids = graphene.List(graphene.UUID)


class CloseBenefitPlanMutation(BaseHistoryModelDeleteMutationMixin, BaseMutation):
    _mutation_class = "CloseBenefitPlanMutation"
    _mutation_module = "social_protection"
    _model = BenefitPlan

    @classmethod
    def _validate_mutation(cls, user, **data):
        if type(user) is AnonymousUser or not user.has_perms(
                SocialProtectionConfig.gql_benefit_plan_close_perms):
            raise ValidationError("mutation.authentication_required")

    @classmethod
    def _mutate(cls, user, **data):
        if "client_mutation_id" in data:
            data.pop('client_mutation_id')
        if "client_mutation_label" in data:
            data.pop('client_mutation_label')

        service = BenefitPlanService(user)
        ids = data.get('ids')
        if ids:
            with transaction.atomic():
                for id in ids:
                    service.close_benefit_plan({'id': id})

    class Input(OpenIMISMutation.Input):
        ids = graphene.List(graphene.UUID)


class CreateGroupBeneficiaryMutation(BaseHistoryModelCreateMutationMixin, BaseMutation):
    _mutation_class = "CreateGroupBeneficiaryMutation"
    _mutation_module = "social_protection"
    _model = GroupBeneficiary

    @classmethod
    def _validate_mutation(cls, user, **data):
        if type(user) is AnonymousUser or not user.has_perms(
                SocialProtectionConfig.gql_beneficiary_create_perms):
            raise ValidationError("mutation.authentication_required")
        check_perms_for_field(
            user, SocialProtectionConfig.gql_schema_create_perms, data, 'json_ext'
        )

    @classmethod
    def _mutate(cls, user, **data):
        if "client_mutation_id" in data:
            data.pop('client_mutation_id')
        if "client_mutation_label" in data:
            data.pop('client_mutation_label')

        service = GroupBeneficiaryService(user)
        if SocialProtectionConfig.gql_check_group_beneficiary_crud:
            res = service.create_create_task(data)
        else:
            res = service.create(data)

        return res if not res['success'] else None

    class Input(CreateGroupBeneficiaryInputType):
        pass


class UpdateGroupBeneficiaryMutation(BaseHistoryModelUpdateMutationMixin, BaseMutation):
    _mutation_class = "UpdateGroupBeneficiaryMutation"
    _mutation_module = "social_protection"
    _model = GroupBeneficiary

    @classmethod
    def _validate_mutation(cls, user, **data):
        super()._validate_mutation(user, **data)
        if type(user) is AnonymousUser or not user.has_perms(
                SocialProtectionConfig.gql_beneficiary_update_perms):
            raise ValidationError("mutation.authentication_required")
        check_perms_for_field(
            user, SocialProtectionConfig.gql_schema_update_perms, data, 'json_ext'
        )

    @classmethod
    def _mutate(cls, user, **data):
        if "date_valid_to" not in data:
            data['date_valid_to'] = None
        if "client_mutation_id" in data:
            data.pop('client_mutation_id')
        if "client_mutation_label" in data:
            data.pop('client_mutation_label')

        service = GroupBeneficiaryService(user)
        if SocialProtectionConfig.gql_check_group_beneficiary_crud:
            res = service.create_update_task(data)
        else:
            res = service.update(data)

        return res if not res['success'] else None

    class Input(UpdateGroupBeneficiaryInputType):
        pass


class DeleteGroupBeneficiaryMutation(BaseHistoryModelDeleteMutationMixin, BaseMutation):
    _mutation_class = "DeleteGroupBeneficiaryMutation"
    _mutation_module = "social_protection"
    _model = GroupBeneficiary

    @classmethod
    def _validate_mutation(cls, user, **data):
        if type(user) is AnonymousUser or not user.id or not user.has_perms(
                SocialProtectionConfig.gql_beneficiary_delete_perms):
            raise ValidationError("mutation.authentication_required")

    @classmethod
    def _mutate(cls, user, **data):
        if "client_mutation_id" in data:
            data.pop('client_mutation_id')
        if "client_mutation_label" in data:
            data.pop('client_mutation_label')

        service = GroupBeneficiaryService(user)

        ids = data.get('ids')
        if not ids:
            return {'success': False, 'message': 'No IDs to delete', 'details': ''}

        with transaction.atomic():
            for obj_id in ids:
                if SocialProtectionConfig.gql_check_group_beneficiary_crud:
                    res = service.create_delete_task({'id': obj_id})
                else:
                    res = service.delete({'id': obj_id, 'user': user})
                if not res['success']:
                    transaction.rollback()
                    return res

    class Input(OpenIMISMutation.Input):
        ids = graphene.List(graphene.UUID)


class CreateLocationBenefitPlanPaymentPointInputType(OpenIMISMutation.Input):
    location_id = graphene.UUID(required=True)
    benefit_plan_id = graphene.UUID(required=True)
    payment_point_name = graphene.String(required=True)
    payment_method = graphene.String(required=False)
    ppm_id = graphene.UUID(required=False)
    
    date_valid_from = graphene.Date(required=True)
    date_valid_to = graphene.Date(required=True)


class UpdateLocationBenefitPlanPaymentPointInputType(CreateLocationBenefitPlanPaymentPointInputType):
    id = graphene.UUID(required=True)


class CreateLocationBenefitPlanPaymentPointMutation(BaseHistoryModelCreateMutationMixin, BaseMutation):
    _mutation_class = "CreateLocationBenefitPlanPaymentPointMutation"
    _mutation_module = "social_protection"
    _model = LocationBenefitPlanPaymentPoint

    @classmethod
    def _validate_mutation(cls, user, **data):
        if type(user) is AnonymousUser or not user.has_perms(
                SocialProtectionConfig.gql_benefit_plan_create_perms):
            raise ValidationError("mutation.authentication_required")

    @classmethod
    def _mutate(cls, user, **data):
        if "client_mutation_id" in data:
            data.pop('client_mutation_id')
        if "client_mutation_label" in data:
            data.pop('client_mutation_label')

        service = LocationBenefitPlanPaymentPointService(user)
        res = service.create(data)
        return res if not res['success'] else None

    class Input(CreateLocationBenefitPlanPaymentPointInputType):
        pass


class UpdateLocationBenefitPlanPaymentPointMutation(BaseHistoryModelUpdateMutationMixin, BaseMutation):
    _mutation_class = "UpdateLocationBenefitPlanPaymentPointMutation"
    _mutation_module = "social_protection"
    _model = LocationBenefitPlanPaymentPoint

    @classmethod
    def _validate_mutation(cls, user, **data):
        super()._validate_mutation(user, **data)
        if type(user) is AnonymousUser or not user.has_perms(
                SocialProtectionConfig.gql_benefit_plan_update_perms):
            raise ValidationError("mutation.authentication_required")

    @classmethod
    def _mutate(cls, user, **data):
        if "date_valid_to" not in data:
            data['date_valid_to'] = None
        if "client_mutation_id" in data:
            data.pop('client_mutation_id')
        if "client_mutation_label" in data:
            data.pop('client_mutation_label')

        service = LocationBenefitPlanPaymentPointService(user)
        res = service.update(data)
        return res if not res['success'] else None

    class Input(UpdateLocationBenefitPlanPaymentPointInputType):
        pass


class DeleteLocationBenefitPlanPaymentPointMutation(BaseHistoryModelDeleteMutationMixin, BaseMutation):
    _mutation_class = "DeleteLocationBenefitPlanPaymentPointMutation"
    _mutation_module = "social_protection"
    _model = LocationBenefitPlanPaymentPoint

    @classmethod
    def _validate_mutation(cls, user, **data):
        if type(user) is AnonymousUser or not user.id or not user.has_perms(
                SocialProtectionConfig.gql_benefit_plan_delete_perms):
            raise ValidationError("mutation.authentication_required")

    @classmethod
    def _mutate(cls, user, **data):
        if "client_mutation_id" in data:
            data.pop('client_mutation_id')
        if "client_mutation_label" in data:
            data.pop('client_mutation_label')

        service = LocationBenefitPlanPaymentPointService(user)
        ids = data.get('ids')
        if not ids:
            return {'success': False, 'message': 'No IDs to delete', 'details': ''}

        with transaction.atomic():
            for obj_id in ids:
                res = service.delete({'id': obj_id, 'user': user})
                if not res['success']:
                    transaction.rollback()
                    return res

    class Input(OpenIMISMutation.Input):
        ids = graphene.List(graphene.UUID)


class BulkUpdateBeneficiaryStatusInputType(OpenIMISMutation.Input):
    beneficiary_ids = graphene.List(graphene.UUID, required=True)
    status = graphene.Field(BeneficiaryStatusEnum, required=True)
    reason = graphene.String(required=False, max_length=255)


class BulkUpdateBeneficiaryStatusMutation(BaseMutation):
    _mutation_class = "BulkUpdateBeneficiaryStatusMutation"
    _mutation_module = "social_protection"

    @classmethod
    def _validate_mutation(cls, user, **data):
        if type(user) is AnonymousUser or not user.has_perms(
                SocialProtectionConfig.gql_beneficiary_update_perms):
            raise ValidationError("mutation.authentication_required")

    @classmethod
    def _mutate(cls, user, **data):
        if "client_mutation_id" in data:
            data.pop('client_mutation_id')
        if "client_mutation_label" in data:
            data.pop('client_mutation_label')

        beneficiary_ids = data.get('beneficiary_ids', [])
        status = data.get('status')
        reason = data.get('reason', '')

        if not beneficiary_ids:
            return {'success': False, 'message': 'No beneficiary IDs provided', 'details': ''}

        service = BeneficiaryService(user)
        
        with transaction.atomic():
            updated_count = 0
            errors = []
            
            for beneficiary_id in beneficiary_ids:
                update_data = {
                    'id': beneficiary_id,
                    'status': status,
                    'json_ext': {'bulk_status_update_reason': reason} if reason else None
                }
                
                if SocialProtectionConfig.gql_check_beneficiary_crud:
                    res = service.create_update_task(update_data)
                else:
                    res = service.update(update_data)
                
                if res['success']:
                    updated_count += 1
                else:
                    errors.append(f"Failed to update beneficiary {beneficiary_id}: {res.get('message', 'Unknown error')}")
            
            if errors:
                transaction.rollback()
                return {'success': False, 'message': 'Bulk update failed', 'details': '; '.join(errors)}
            
            return {'success': True, 'message': f'Successfully updated {updated_count} beneficiaries'}

    class Input(BulkUpdateBeneficiaryStatusInputType):
        pass


class BulkUpdateGroupBeneficiaryStatusInputType(OpenIMISMutation.Input):
    group_beneficiary_ids = graphene.List(graphene.UUID, required=True)
    status = graphene.Field(BeneficiaryStatusEnum, required=True)
    reason = graphene.String(required=False, max_length=255)


class BulkUpdateGroupBeneficiaryStatusMutation(BaseMutation):
    _mutation_class = "BulkUpdateGroupBeneficiaryStatusMutation"
    _mutation_module = "social_protection"

    @classmethod
    def _validate_mutation(cls, user, **data):
        if type(user) is AnonymousUser or not user.has_perms(
                SocialProtectionConfig.gql_beneficiary_update_perms):
            raise ValidationError("mutation.authentication_required")

    @classmethod
    def _mutate(cls, user, **data):
        if "client_mutation_id" in data:
            data.pop('client_mutation_id')
        if "client_mutation_label" in data:
            data.pop('client_mutation_label')

        group_beneficiary_ids = data.get('group_beneficiary_ids', [])
        status = data.get('status')
        reason = data.get('reason', '')

        if not group_beneficiary_ids:
            return {'success': False, 'message': 'No group beneficiary IDs provided', 'details': ''}

        service = GroupBeneficiaryService(user)
        
        with transaction.atomic():
            updated_count = 0
            errors = []
            
            for group_beneficiary_id in group_beneficiary_ids:
                update_data = {
                    'id': group_beneficiary_id,
                    'status': status,
                    'json_ext': {'bulk_status_update_reason': reason} if reason else None
                }
                
                if SocialProtectionConfig.gql_check_group_beneficiary_crud:
                    res = service.create_update_task(update_data)
                else:
                    res = service.update(update_data)
                
                if res['success']:
                    updated_count += 1
                else:
                    errors.append(f"Failed to update group beneficiary {group_beneficiary_id}: {res.get('message', 'Unknown error')}")
            
            if errors:
                transaction.rollback()
                return {'success': False, 'message': 'Bulk update failed', 'details': '; '.join(errors)}
            
            return {'success': True, 'message': f'Successfully updated {updated_count} group beneficiaries'}

    class Input(BulkUpdateGroupBeneficiaryStatusInputType):
        pass


class CSVUpdateGroupBeneficiaryStatusInputType(OpenIMISMutation.Input):
    csv_file = graphene.String(required=True, description="Base64 encoded CSV file content")
    status = graphene.Field(BeneficiaryStatusEnum, required=True)
    benefit_plan_id = graphene.UUID(required=True)
    reason = graphene.String(required=False, max_length=255)


class CSVUpdateGroupBeneficiaryStatusMutation(BaseMutation):
    _mutation_class = "CSVUpdateGroupBeneficiaryStatusMutation"
    _mutation_module = "social_protection"

    @classmethod
    def _validate_mutation(cls, user, **data):
        if type(user) is AnonymousUser or not user.has_perms(
                SocialProtectionConfig.gql_beneficiary_update_perms):
            raise ValidationError("mutation.authentication_required")

    @classmethod
    def _mutate(cls, user, **data):
        import base64
        import csv
        import io
        from individual.models import Group
        from social_protection.signals.on_confirm_enrollment_of_group import on_confirm_enrollment_of_group
        from datetime import datetime
        
        if "client_mutation_id" in data:
            data.pop('client_mutation_id')
        if "client_mutation_label" in data:
            data.pop('client_mutation_label')

        csv_file_base64 = data.get('csv_file')
        status = data.get('status')
        benefit_plan_id = data.get('benefit_plan_id')
        reason = data.get('reason', '')

        if not csv_file_base64:
            return {'success': False, 'message': 'No CSV file provided', 'details': ''}

        try:
            # Decode base64 to get CSV content
            csv_content = base64.b64decode(csv_file_base64).decode('utf-8-sig')  # utf-8-sig removes BOM
            csv_reader = csv.DictReader(io.StringIO(csv_content))
            
            # Clean fieldnames to remove any BOM or extra whitespace
            if csv_reader.fieldnames:
                cleaned_fieldnames = []
                for field in csv_reader.fieldnames:
                    # Remove BOM and strip whitespace
                    cleaned_field = field.replace('\ufeff', '').strip()
                    cleaned_fieldnames.append(cleaned_field)
                csv_reader.fieldnames = cleaned_fieldnames
            
            # Validate CSV has required columns
            if 'group_code' not in csv_reader.fieldnames:
                return {'success': False, 'message': 'CSV must contain "group_code" column', 'details': ''}
            
            # Collect all group codes and additional data
            groups_to_enroll = []
            groups_to_update = []
            not_found_codes = []
            csv_data_by_group = {}  # Store CSV data for each group
            
            for row in csv_reader:
                # Clean row keys to handle any BOM or whitespace issues
                cleaned_row = {}
                for k, v in row.items():
                    cleaned_key = k.replace('\ufeff', '').strip() if k else k
                    cleaned_row[cleaned_key] = v
                
                group_code = cleaned_row.get('group_code', '').strip()
                if not group_code:
                    continue
                
                # Collect all CSV fields
                csv_fields = {k: v for k, v in cleaned_row.items() if k != 'group_code' and v}
                
                # Find group by code
                try:
                    group = Group.objects.get(code=group_code, is_deleted=False)
                    
                    # Check if group is already a beneficiary
                    existing_beneficiary = GroupBeneficiary.objects.filter(
                        group=group,
                        benefit_plan_id=benefit_plan_id,
                        is_deleted=False
                    ).first()
                    
                    # Store CSV data for this group
                    csv_data_by_group[group.id] = csv_fields
                    
                    if existing_beneficiary:
                        groups_to_update.append((existing_beneficiary, csv_fields))
                    else:
                        groups_to_enroll.append(group)
                        
                except Group.DoesNotExist:
                    not_found_codes.append(group_code)
            
            # Process results
            enrolled_count = 0
            updated_count = 0
            
            # Handle new enrollments using the existing enrollment workflow
            if groups_to_enroll:
                # Prepare the result dict similar to what the enrollment expects
                enrollment_result = {
                    'benefit_plan_id': benefit_plan_id,
                    'status': status,
                    'user': user,
                    'groups_not_assigned_to_selected_programme': Group.objects.filter(
                        id__in=[g.id for g in groups_to_enroll]
                    )
                }
                
                # Update group json_ext with CSV data and reason before enrollment
                for group in groups_to_enroll:
                    if group.id in csv_data_by_group:
                        # Merge CSV data into group's json_ext
                        group_json_ext = group.json_ext or {}
                        group_json_ext['csv_enrollment_data'] = csv_data_by_group[group.id]
                        group_json_ext['csv_enrollment_date'] = str(datetime.now())
                        
                        # Add reason
                        if reason:
                            try:
                                import json as json_module
                                reason_json = json_module.loads(reason)
                                if isinstance(reason_json, dict):
                                    group_json_ext.update(reason_json)
                                else:
                                    group_json_ext['csv_enrollment_reason'] = reason
                            except:
                                group_json_ext['csv_enrollment_reason'] = reason
                        
                        group.json_ext = group_json_ext
                        group.save(user=user)
                
                # Call the enrollment function
                on_confirm_enrollment_of_group(result=enrollment_result)
                enrolled_count = len(groups_to_enroll)
            
            # Handle updates for existing beneficiaries
            if groups_to_update:
                service = GroupBeneficiaryService(user)
                
                with transaction.atomic():
                    for group_beneficiary, csv_fields in groups_to_update:
                        # Update status and json_ext
                        update_data = {
                            'id': group_beneficiary.id,
                            'status': status,
                            'json_ext': group_beneficiary.json_ext.copy() if group_beneficiary.json_ext else {}
                        }
                        
                        # Add CSV update metadata
                        update_data['json_ext']['csv_status_update_date'] = str(datetime.now())
                        update_data['json_ext']['csv_fields'] = csv_fields
                        
                        # Check if reason is valid JSON and merge it
                        if reason:
                            try:
                                import json as json_module
                                reason_json = json_module.loads(reason)
                                if isinstance(reason_json, dict):
                                    update_data['json_ext'].update(reason_json)
                                else:
                                    update_data['json_ext']['csv_status_update_reason'] = reason
                            except:
                                update_data['json_ext']['csv_status_update_reason'] = reason
                        
                        res = service.update(update_data)
                        if res['success']:
                            updated_count += 1
            
            # Prepare response message
            details = []
            if enrolled_count > 0:
                details.append(f'Enrolled {enrolled_count} new group beneficiaries')
            if updated_count > 0:
                details.append(f'Updated {updated_count} existing group beneficiaries')
            if not_found_codes:
                details.append(f'Group codes not found: {", ".join(not_found_codes[:10])}{"..." if len(not_found_codes) > 10 else ""}')
            
            return {'success': True, 'message': '; '.join(details) if details else 'No changes made'}
            
        except Exception as e:
            return {'success': False, 'message': f'CSV processing error: {str(e)}', 'details': ''}

    class Input(CSVUpdateGroupBeneficiaryStatusInputType):
        pass
