from django.conf import settings
from django.contrib.auth.models import AnonymousUser
from django.db import models
from django.db.models import Func
from django.utils.translation import gettext as _
from django.core.exceptions import ValidationError

from core import models as core_models
from core.models import UUIDModel, ObjectMutation, MutationLog
from individual.models import Individual, Group, IndividualDataSourceUpload
from location.models import Location


class BeneficiaryStatus(models.TextChoices):
    POTENTIAL = "POTENTIAL", _("POTENTIAL")
    ACTIVE = "ACTIVE", _("ACTIVE")
    GRADUATED = "GRADUATED", _("GRADUATED")
    SUSPENDED = "SUSPENDED", _("SUSPENDED")


class BenefitPlan(core_models.HistoryBusinessModel):
    class BenefitPlanType(models.TextChoices):
        INDIVIDUAL_TYPE = "INDIVIDUAL", _("INDIVIDUAL")
        GROUP_TYPE = "GROUP", _("GROUP")

    code = models.CharField(max_length=8, null=False)
    name = models.CharField(max_length=255, null=False)
    max_beneficiaries = models.SmallIntegerField(null=True, blank=True)
    ceiling_per_beneficiary = models.DecimalField(
        max_digits=18, decimal_places=2, blank=True, null=True,
    )
    institution = models.CharField(max_length=255, null=True, blank=True)
    beneficiary_data_schema = models.JSONField(null=True, blank=True)
    type = models.CharField(
        max_length=100, choices=BenefitPlanType.choices, default=BenefitPlanType.INDIVIDUAL_TYPE, null=False
    )
    description = models.CharField(max_length=1024, null=True, blank=True)

    def __str__(self):
        return f'Benefit Plan {self.code}'


class BenefitPlanMutation(UUIDModel, ObjectMutation):
    benefit_plan = models.ForeignKey(BenefitPlan, models.DO_NOTHING, related_name='mutations')
    mutation = models.ForeignKey(MutationLog, models.DO_NOTHING, related_name='benefit_plan')


class Beneficiary(core_models.HistoryBusinessModel):
    individual = models.ForeignKey(Individual, models.DO_NOTHING, null=False)
    benefit_plan = models.ForeignKey(BenefitPlan, models.DO_NOTHING, null=False)
    status = models.CharField(max_length=100, choices=BeneficiaryStatus.choices, null=False)

    json_ext = models.JSONField(db_column="Json_ext", blank=True, default=dict)

    def clean(self):
        if self.benefit_plan.type != BenefitPlan.BenefitPlanType.INDIVIDUAL_TYPE:
            raise ValidationError(_("Beneficiary must be associated with an individual benefit plan."))
        super().clean()

    def __str__(self):
        return f'{self.individual.first_name} {self.individual.last_name}'
    
    @classmethod
    def get_queryset(cls, queryset, user):
        if queryset is None:
            queryset = cls.objects.all()

        individuals = Individual.objects.filter(
            id__in=queryset.values('individual_id')
        ).distinct()
    
        individual_queryset = Individual.get_queryset(individuals, user)
        return queryset.filter(individual__in=individual_queryset)


class BenefitPlanDataUploadRecords(core_models.HistoryModel):
    data_upload = models.ForeignKey(IndividualDataSourceUpload, models.DO_NOTHING, null=False)
    benefit_plan = models.ForeignKey(BenefitPlan, models.DO_NOTHING, null=False)
    workflow = models.CharField(max_length=50)

    def __str__(self):
        return f"{self.benefit_plan.code} {self.data_upload.source_name} {self.workflow} {self.date_created}"


class GroupBeneficiary(core_models.HistoryBusinessModel):
    group = models.ForeignKey(Group, models.DO_NOTHING, null=False)
    benefit_plan = models.ForeignKey(BenefitPlan, models.DO_NOTHING, null=False)
    status = models.CharField(max_length=100, choices=BeneficiaryStatus.choices, null=False)

    json_ext = models.JSONField(db_column="Json_ext", blank=True, default=dict)

    def clean(self):
        if self.benefit_plan.type != BenefitPlan.BenefitPlanType.GROUP_TYPE:
            raise ValidationError(_("Group beneficiary must be associated with a benefit plan type = GROUP."))

        super().clean()
    
    @classmethod
    def get_queryset(cls, queryset, user):
        if queryset is None:
            queryset = cls.objects.all()

        groups = Group.objects.filter(
            id__in=queryset.values('group_id')
        ).distinct()
    
        group_queryset = Group.get_queryset(groups, user)
        return queryset.filter(group__in=group_queryset)


class Activity(core_models.HistoryBusinessModel):
    name = models.CharField(max_length=255, null=False, unique=True)

    class Meta:
        verbose_name = "Activity"
        verbose_name_plural = "Activities"


class ProjectStatus(models.TextChoices):
    PREPARATION = "PREPARATION", _("PREPARATION")
    IN_PROGRESS = "IN PROGRESS", _("IN PROGRESS")
    COMPLETED = "COMPLETED", _("COMPLETED")


class Project(core_models.HistoryBusinessModel):
    benefit_plan = models.ForeignKey(BenefitPlan, models.DO_NOTHING, null=False)
    name = models.CharField(max_length=255, null=False)
    status = models.CharField(
        max_length=100,
        choices=ProjectStatus.choices,
        default=ProjectStatus.PREPARATION,
        null=False
    )
    activity = models.ForeignKey(Activity, models.DO_NOTHING, null=False)
    location = models.ForeignKey(Location, models.DO_NOTHING, null=False)
    target_beneficiaries = models.SmallIntegerField(null=False)
    working_days = models.SmallIntegerField(null=False)


class JSONUpdate(Func):
    function = 'JSONB_SET'
    arity = 3
