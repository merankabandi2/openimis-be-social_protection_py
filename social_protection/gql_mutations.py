import graphene as graphene
from django.contrib.auth.models import AnonymousUser
from django.core.exceptions import ValidationError
from django.db import transaction

from core.gql.gql_mutations.base_mutation import BaseHistoryModelCreateMutationMixin, BaseMutation, \
    BaseHistoryModelUpdateMutationMixin, BaseHistoryModelDeleteMutationMixin
from core.schema import OpenIMISMutation
from social_protection.apps import SocialProtectionConfig
from social_protection.models import (
    BenefitPlan, Project, Activity,
    Beneficiary, GroupBeneficiary, BeneficiaryStatus, BenefitPlanMutation
)
from social_protection.services import (
    BenefitPlanService, ProjectService,
    BeneficiaryService, GroupBeneficiaryService
)
from location.models import Location


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
    class BeneficiaryStatusEnum(graphene.Enum):
        POTENTIAL = BeneficiaryStatus.POTENTIAL
        ACTIVE = BeneficiaryStatus.ACTIVE
        GRADUATED = BeneficiaryStatus.GRADUATED
        SUSPENDED = BeneficiaryStatus.SUSPENDED

    status = graphene.Field(BeneficiaryStatusEnum, required=True)
    benefit_plan_id = graphene.UUID(required=False)

    date_valid_from = graphene.Date(required=False)
    date_valid_to = graphene.Date(required=False)
    json_ext = graphene.types.json.JSONString(required=False)

    def resolve_status(self, info):
        return self.status


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

class CreateProjectInputType(OpenIMISMutation.Input):
    benefit_plan_id = graphene.ID(required=True)
    name = graphene.String(required=True)
    status = graphene.String(required=False)
    activity_id = graphene.ID(required=True)
    location_id = graphene.ID(required=True)
    target_beneficiaries = graphene.Int(required=True)


class CreateProjectMutation(BaseHistoryModelCreateMutationMixin, BaseMutation):
    _mutation_class = "CreateProjectMutation"
    _mutation_module = "social_protection"
    _model = Project

    @classmethod
    def _validate_mutation(cls, user, **data):
        if isinstance(user, AnonymousUser) or not user.has_perms(
            SocialProtectionConfig.gql_project_create_perms
        ):
            raise ValidationError("mutation.authentication_required")

    @classmethod
    def _mutate(cls, user, **data):
        if "client_mutation_id" in data:
            data.pop("client_mutation_id")
        if "client_mutation_label" in data:
            data.pop("client_mutation_label")

        data["benefit_plan"] = BenefitPlan.objects.get(id=data.pop("benefit_plan_id"))
        data["activity"] = Activity.objects.get(id=data.pop("activity_id"))
        data["location"] = Location.objects.get(id=data.pop("location_id"))
        data.setdefault("status", Project._meta.get_field("status").get_default())

        service = ProjectService(user)
        res = service.create(data)

        return res if not res['success'] else None

    class Input(CreateProjectInputType):
        pass

