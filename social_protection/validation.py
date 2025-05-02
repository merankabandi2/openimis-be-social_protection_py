from django.core.exceptions import ValidationError
from django.utils.translation import gettext as _

from core.utils import validate_json_schema
from core.validation import BaseModelValidation
from social_protection.models import Beneficiary, BenefitPlan, Project


class BenefitPlanValidation(BaseModelValidation):
    OBJECT_TYPE = BenefitPlan

    @classmethod
    def validate_create(cls, user, **data):
        errors = validate_benefit_plan(data)
        if errors:
            raise ValidationError(errors)
        super().validate_create(user, **data)

    @classmethod
    def validate_update(cls, user, **data):
        uuid = data.get('id')
        errors = validate_benefit_plan(data, uuid)
        if errors:
            raise ValidationError(errors)
        super().validate_update(user, **data)

    @classmethod
    def validate_delete(cls, user, **data):
        super().validate_delete(user, **data)


def validate_benefit_plan(data, uuid=None):
    validations = [
        *validate_not_empty_field(data.get("code"), "code"),
        *validate_bf_unique_code(data.get('code'), uuid),
        *validate_not_empty_field(data.get("name"), "name"),
        *validate_bf_unique_name(data.get('name'), uuid)
    ]

    beneficiary_data_schema = data.get('beneficiary_data_schema')
    if beneficiary_data_schema:
        validations.extend(validate_json_schema(beneficiary_data_schema))

    return validations


def validate_bf_unique_code(code, uuid=None):
    instance = BenefitPlan.objects.filter(code=code, is_deleted=False).exclude(id=uuid).first()
    if instance:
        return [{"message": _("social_protection.validation.benefit_plan.code_exists" % {
            'code': code
        })}]
    return []


def validate_bf_unique_name(name, uuid=None):
    instance = BenefitPlan.objects.filter(name=name, is_deleted=False).exclude(id=uuid).first()
    if instance:
        return [{"message": _("social_protection.validation.benefit_plan.name_exists" % {
            'name': name
        })}]
    return []


def validate_not_empty_field(string, field):
    if not string:
        return [{"message": _("social_protection.validation.field_empty") % {
            'field': field
        }}]
    return []


class BeneficiaryValidation(BaseModelValidation):
    OBJECT_TYPE = Beneficiary


class GroupBeneficiaryValidation(BaseModelValidation):
    OBJECT_TYPE = Beneficiary


def validate_project_unique_name(name, benefit_plan_id, uuid=None):
    instance = Project.objects.filter(
        name=name, benefit_plan__id=benefit_plan_id, is_deleted=False
    ).exclude(id=uuid).first()
    if instance:
        return [{"message": _("social_protection.validation.project.name_exists" % {
            'name': name
        })}]
    return []
