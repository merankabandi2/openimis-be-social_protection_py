import graphene
from django.contrib.auth.models import AnonymousUser
from graphene import ObjectType
from graphene_django import DjangoObjectType
import django_filters
from graphene_django.filter import DjangoFilterConnectionField

from contribution_plan.models import PaymentPlan
from core import prefix_filterset, ExtendedConnection
from individual.gql_queries import IndividualGQLType, GroupGQLType, \
    IndividualDataSourceUploadGQLType
from social_protection.apps import SocialProtectionConfig
from social_protection.models import Beneficiary, BenefitPlan, GroupBeneficiary, BenefitPlanDataUploadRecords, LocationBenefitPlanPaymentPoint
from location.gql_queries import LocationGQLType

from location.models import Location
from social_protection.models import (
    Beneficiary, BenefitPlan, GroupBeneficiary, BenefitPlanDataUploadRecords,
    Activity, Project,
)


def _have_permissions(user, permission):
    if isinstance(user, AnonymousUser):
        return False
    if not user.id:
        return False
    return user.has_perms(permission)


class JsonExtMixin:
    def resolve_json_ext(self, info):
        if _have_permissions(info.context.user, SocialProtectionConfig.gql_schema_search_perms):
            return self.json_ext
        return None


class BenefitPlanGQLType(DjangoObjectType, JsonExtMixin):
    uuid = graphene.String(source='uuid')
    has_payment_plans = graphene.Boolean()

    class Meta:
        model = BenefitPlan
        interfaces = (graphene.relay.Node,)
        filter_fields = {
            "id": ["exact"],
            "code": ["exact", "iexact", "startswith", "istartswith", "contains", "icontains"],
            "name": ["exact", "iexact", "startswith", "istartswith", "contains", "icontains"],
            "date_valid_from": ["exact", "lt", "lte", "gt", "gte"],
            "date_valid_to": ["exact", "lt", "lte", "gt", "gte"],
            "max_beneficiaries": ["exact", "lt", "lte", "gt", "gte"],
            "institution": ["exact", "iexact", "startswith", "istartswith", "contains", "icontains"],
            "type": ["exact", "iexact", "startswith", "istartswith", "contains", "icontains"],

            "date_created": ["exact", "lt", "lte", "gt", "gte"],
            "date_updated": ["exact", "lt", "lte", "gt", "gte"],
            "is_deleted": ["exact"],
            "version": ["exact"],
            "description": ["exact", "iexact", "startswith", "istartswith", "contains", "icontains"],
        }
        connection_class = ExtendedConnection

    def resolve_beneficiary_data_schema(self, info):
        if _have_permissions(info.context.user, SocialProtectionConfig.gql_schema_search_perms):
            return self.beneficiary_data_schema
        return None

    def resolve_has_payment_plans(self, info):
        return PaymentPlan.objects.filter(benefit_plan_id=self.id).exists()

class BeneficiaryFilter(django_filters.FilterSet):
    is_eligible = django_filters.BooleanFilter(method='filter_is_eligible')

    class Meta:
        model = Beneficiary
        fields = {
            "id": ["exact"],
            "status": ["exact", "iexact", "startswith", "istartswith", "contains", "icontains"],
            "date_valid_from": ["exact", "lt", "lte", "gt", "gte"],
            "date_valid_to": ["exact", "lt", "lte", "gt", "gte"],
            **prefix_filterset("individual__", IndividualGQLType._meta.filter_fields),
            **prefix_filterset("benefit_plan__", BenefitPlanGQLType._meta.filter_fields),
            "date_created": ["exact", "lt", "lte", "gt", "gte"],
            "date_updated": ["exact", "lt", "lte", "gt", "gte"],
            "is_deleted": ["exact"],
            "version": ["exact"],
        }

    def filter_is_eligible(self, queryset, name, value):
        return queryset.filter(is_eligible=value)


class BeneficiaryGQLType(DjangoObjectType, JsonExtMixin):
    uuid = graphene.String(source='uuid')
    is_eligible = graphene.Boolean()

    class Meta:
        model = Beneficiary
        interfaces = (graphene.relay.Node,)
        filterset_class = BeneficiaryFilter
        connection_class = ExtendedConnection

    def resolve_is_eligible(self, info):
        return self.is_eligible


class GroupBeneficiaryFilter(django_filters.FilterSet):
    is_eligible = django_filters.BooleanFilter(method='filter_is_eligible')

    class Meta:
        model = GroupBeneficiary
        fields = {
            "id": ["exact"],
            "status": ["exact", "iexact", "startswith", "istartswith", "contains", "icontains"],
            "date_valid_from": ["exact", "lt", "lte", "gt", "gte"],
            "date_valid_to": ["exact", "lt", "lte", "gt", "gte"],
            **prefix_filterset("group__", GroupGQLType._meta.filter_fields),
            **prefix_filterset("benefit_plan__", BenefitPlanGQLType._meta.filter_fields),
            "date_created": ["exact", "lt", "lte", "gt", "gte"],
            "date_updated": ["exact", "lt", "lte", "gt", "gte"],
            "is_deleted": ["exact"],
            "version": ["exact"],
        }

    def filter_is_eligible(self, queryset, name, value):
        return queryset.filter(is_eligible=value)


class GroupBeneficiaryGQLType(DjangoObjectType, JsonExtMixin):
    uuid = graphene.String(source='uuid')
    is_eligible = graphene.Boolean()

    class Meta:
        model = GroupBeneficiary
        interfaces = (graphene.relay.Node,)
        filterset_class = GroupBeneficiaryFilter
        connection_class = ExtendedConnection

    def resolve_is_eligible(self, info):
        return self.is_eligible


class BenefitPlanDataUploadQGLType(DjangoObjectType, JsonExtMixin):
    uuid = graphene.String(source='uuid')

    class Meta:
        model = BenefitPlanDataUploadRecords
        interfaces = (graphene.relay.Node,)
        filter_fields = {
            "id": ["exact"],
            "date_created": ["exact", "lt", "lte", "gt", "gte"],
            "date_updated": ["exact", "lt", "lte", "gt", "gte"],
            "is_deleted": ["exact"],
            "version": ["exact"],
            "workflow": ["exact", "iexact", "startswith", "istartswith", "contains", "icontains"],
            **prefix_filterset("data_upload__", IndividualDataSourceUploadGQLType._meta.filter_fields),
            **prefix_filterset("benefit_plan__", BenefitPlanGQLType._meta.filter_fields),
        }
        connection_class = ExtendedConnection


class BenefitPlanSchemaFieldsGQLType(ObjectType):
    schema_fields = graphene.List(graphene.String)

    def resolve_schema_fields(self, info, **kwargs):
        schemas = self.values_list("beneficiary_data_schema__properties", flat=True)
        field_list = set(
            f'json_ext__{field}'
            for schema in schemas  # Iterate over each schema
            if schema  # Ensure the schema is not None or empty
            for field in schema  # Iterate over fields in the schema
        )
        return field_list


class BenefitPlanLocationGQLType(LocationGQLType):
    count_selected = graphene.Int()
    count_active = graphene.Int()
    count_suspended = graphene.Int()
    count_all= graphene.Int()

    class Meta:
        model = Location
        interfaces = (graphene.relay.Node,)
        filter_fields = {
            "id": ["exact"],
            "code": ["exact", "istartswith", "icontains", "iexact", "ne"],
            "name": ["exact", "istartswith", "icontains", "iexact", "ne"],
            "type": ["exact"],
            "parent__id": ["exact", "in"],  # can't import itself!
        }
        connection_class = ExtendedConnection

class BenefitPlanHistoryGQLType(DjangoObjectType, JsonExtMixin):
    uuid = graphene.String(source='uuid')
    has_payment_plans = graphene.Boolean()

    def resolve_user_updated(self, info):
        return self.user_updated

    class Meta:
        model = BenefitPlan.history.model
        interfaces = (graphene.relay.Node,)
        filter_fields = {
            "id": ["exact"],
            "code": ["exact", "iexact", "startswith", "istartswith", "contains", "icontains"],
            "name": ["exact", "iexact", "startswith", "istartswith", "contains", "icontains"],
            "date_valid_from": ["exact", "lt", "lte", "gt", "gte"],
            "date_valid_to": ["exact", "lt", "lte", "gt", "gte"],
            "max_beneficiaries": ["exact", "lt", "lte", "gt", "gte"],
            "institution": ["exact", "iexact", "startswith", "istartswith", "contains", "icontains"],

            "date_created": ["exact", "lt", "lte", "gt", "gte"],
            "date_updated": ["exact", "lt", "lte", "gt", "gte"],
            "is_deleted": ["exact"],
            "version": ["exact"],
            "description": ["exact", "iexact", "startswith", "istartswith", "contains", "icontains"],
        }
        connection_class = ExtendedConnection

    def resolve_beneficiary_data_schema(self, info):
        if _have_permissions(info.context.user, SocialProtectionConfig.gql_schema_search_perms):
            return self.beneficiary_data_schema
        return None

    def resolve_has_payment_plans(self, info):
        return PaymentPlan.objects.filter(benefit_plan_id=self.id).exists()


class LocationBenefitPlanPaymentPointGQLType(DjangoObjectType, JsonExtMixin):
    uuid = graphene.String(source='uuid')
    
    class Meta:
        model = LocationBenefitPlanPaymentPoint
        interfaces = (graphene.relay.Node,)
        filter_fields = {
            "id": ["exact"],
            "payment_point_name": ["exact", "iexact", "startswith", "istartswith", "contains", "icontains"],
            "payment_method": ["exact", "iexact", "startswith", "istartswith", "contains", "icontains"],
            **prefix_filterset("location__", LocationGQLType._meta.filter_fields),
            **prefix_filterset("benefit_plan__", BenefitPlanGQLType._meta.filter_fields),
            **prefix_filterset("ppm__", {"username": ["exact", "icontains", "istartswith"]}),
            "date_created": ["exact", "lt", "lte", "gt", "gte"],
            "date_updated": ["exact", "lt", "lte", "gt", "gte"],
            "is_deleted": ["exact"],
            "version": ["exact"],
        }
        connection_class = ExtendedConnection

class ActivityFilter(django_filters.FilterSet):
    class Meta:
        model = Activity
        fields = {
            "id": ["exact"],
            "name": ["exact", "iexact", "startswith", "istartswith", "contains", "icontains"],
            "date_created": ["exact", "lt", "lte", "gt", "gte"],
            "date_updated": ["exact", "lt", "lte", "gt", "gte"],
            "is_deleted": ["exact"],
            "version": ["exact"],
        }


class ActivityGQLType(DjangoObjectType, JsonExtMixin):
    uuid = graphene.String(source='uuid')

    class Meta:
        model = Activity
        interfaces = (graphene.relay.Node,)
        filterset_class = ActivityFilter
        connection_class = ExtendedConnection


class ProjectFilter(django_filters.FilterSet):
    class Meta:
        model = Project
        fields = {
            "id": ["exact"],
            "name": ["exact", "iexact", "startswith", "istartswith", "contains", "icontains"],
            'status': ['exact', 'icontains'],
            'benefit_plan__id': ['exact'],
            'activity__id': ['exact'],
            'location__id': ['exact'],
            'target_beneficiaries': ['exact', 'gte', 'lte'],
            'working_days': ['exact', 'gte', 'lte'],
            "date_created": ["exact", "lt", "lte", "gt", "gte"],
            "date_updated": ["exact", "lt", "lte", "gt", "gte"],
            "is_deleted": ["exact"],
            "version": ["exact"],
        }

class ProjectGQLType(DjangoObjectType, JsonExtMixin):
    uuid = graphene.String(source='uuid')

    class Meta:
        model = Project
        interfaces = (graphene.relay.Node,)
        filterset_class = ProjectFilter
        connection_class = ExtendedConnection
