from django.db import models
from django.db.models import Func
from django.utils.translation import gettext as _
from django.core.exceptions import ValidationError
from django.core.validators import MinValueValidator, MaxValueValidator

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
    @classmethod
    def get_rights(cls, action):
        """
        The rights governing an action on a programme, for GraphQL as for REST.

        Redeclares nothing: the rights table is
        `social_protection.apps.DJANGO_PERMS`, by entity then by action, and
        `configured_perms` reads the *configured* value there - the one
        ModuleConfiguration may have overridden - and not the declared default. The
        read happens inside the method and never at import time: the `_perms` keys only
        hold their value after `ready()`, and a snapshot taken at import would capture
        an empty list, which `has_perms` grants to everybody.

        "close" (160005) is available just like the four canonical actions.
        """
        from social_protection.apps import configured_perms

        return configured_perms("benefitPlan", action)

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


class Activity(core_models.HistoryBusinessModel):
    @classmethod
    def get_rights(cls, action):
        """Only "query" (208001) is declared: the activity has no mutation."""
        from social_protection.apps import configured_perms

        return configured_perms("activity", action)

    name = models.CharField(max_length=255, null=False, unique=True)

    class Meta:
        verbose_name = "Activity"
        verbose_name_plural = "Activities"


class ProjectStatus(models.TextChoices):
    PREPARATION = "PREPARATION", _("PREPARATION")
    IN_PROGRESS = "IN_PROGRESS", _("IN PROGRESS")
    COMPLETED = "COMPLETED", _("COMPLETED")


class Project(core_models.HistoryBusinessModel):
    @classmethod
    def get_rights(cls, action):
        """
        The rights of an action on a project. Besides the four canonical ones, the
        entity carries two business actions: "enroll" (209005) and "timeEntry"
        (209006).
        """
        from social_protection.apps import configured_perms

        return configured_perms("project", action)

    # The location is picked at any level, down to a village: the scope matches
    # every level, so a project is visible in the districts that contain it.
    row_scope = core_models.LocationScope("location", loc_types=("R", "D", "W", "V"))

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
    allows_multiple_enrollments = models.BooleanField(default=False)


class ProjectMutation(UUIDModel, ObjectMutation):
    project = models.ForeignKey(Project, models.DO_NOTHING, related_name='mutations')
    mutation = models.ForeignKey(MutationLog, models.DO_NOTHING, related_name='project')


class BeneficiaryProjectEnrollment(core_models.HistoryBusinessModel):
    # No `scope_parent = "project"`: inheritance takes the action *of the same name*
    # on the parent, so creating an enrolment would go through `project.create`
    # (209002), whereas the call site (ProjectEnrollmentMutation) requires
    # `project.enroll` (209005). So the mapping is written out explicitly.
    @classmethod
    def get_rights(cls, action):
        from social_protection.apps import configured_perms

        if action in ("create", "delete", "enroll"):
            return configured_perms("project", "enroll")
        # None = no rule declared: the caller must fail closed. Enrolments are only
        # read through the project or the beneficiary.
        return None

    row_scope = core_models.ParentScope("beneficiary")

    beneficiary = models.ForeignKey(
        'Beneficiary',
        models.DO_NOTHING,
        related_name='project_enrollments',
        null=False,
    )
    project = models.ForeignKey(
        Project,
        models.DO_NOTHING,
        related_name='beneficiary_enrollments',
        null=False,
    )

    class Meta:
        unique_together = ('beneficiary', 'project')
        verbose_name = _("Beneficiary Project Enrollment")
        verbose_name_plural = _("Beneficiary Project Enrollments")

    def clean(self):
        if self.beneficiary.status != BeneficiaryStatus.ACTIVE:
            raise ValidationError(_("Only ACTIVE beneficiaries can be enrolled in a project."))
        if self.project.benefit_plan_id != self.beneficiary.benefit_plan_id:
            raise ValidationError(_("Beneficiary and project must belong to the same program."))
        super().clean()

    def __str__(self):
        return f"{self.beneficiary} - {self.project.name}"


class GroupBeneficiaryProjectEnrollment(core_models.HistoryBusinessModel):
    # Same right as the individual enrolment: `project.enroll` (209005) covers both
    # variants, which is what ProjectGroupEnrollmentMutation does.
    @classmethod
    def get_rights(cls, action):
        from social_protection.apps import configured_perms

        if action in ("create", "delete", "enroll"):
            return configured_perms("project", "enroll")
        return None

    row_scope = core_models.ParentScope("group_beneficiary")

    group_beneficiary = models.ForeignKey(
        'GroupBeneficiary',
        models.DO_NOTHING,
        related_name='project_enrollments',
        null=False,
    )
    project = models.ForeignKey(
        Project,
        models.DO_NOTHING,
        related_name='group_beneficiary_enrollments',
        null=False,
    )

    class Meta:
        unique_together = ('group_beneficiary', 'project')
        verbose_name = _("Group Beneficiary Project Enrollment")
        verbose_name_plural = _("Group Beneficiary Project Enrollments")

    def clean(self):
        if self.group_beneficiary.status != BeneficiaryStatus.ACTIVE:
            raise ValidationError(_("Only ACTIVE group beneficiaries can be enrolled in a project."))
        if self.project.benefit_plan_id != self.group_beneficiary.benefit_plan_id:
            raise ValidationError(_("Group beneficiary and project must belong to the same program."))
        super().clean()

    def __str__(self):
        return f"{self.group_beneficiary.group.code} - {self.project.name}"


class Beneficiary(core_models.HistoryBusinessModel):
    @classmethod
    def get_rights(cls, action):
        """
        The rights of an action on a beneficiary (170001-170004).

        A known borrowing, not fixed here: the six REST views of `views.py` are guarded
        by the `individual` module's rights (159001/159002) and not by these. This
        access point gives the *expected* right; changing the call sites is another
        batch of work.
        """
        from social_protection.apps import configured_perms

        return configured_perms("beneficiary", action)

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


class AbstractProjectTimeEntry(core_models.HistoryBusinessModel):
    """
    Base model for recording daily percent completion for enrollments in projects.
    Subclasses must implement `_get_enrollment_instance()`.
    """
    # Sequential workday number within the project (1..working_days).
    day_number = models.PositiveSmallIntegerField(validators=[MinValueValidator(1)])
    # Percentage of work completed for this day (0–100).
    percent_complete = models.IntegerField(
        validators=[MinValueValidator(0), MaxValueValidator(100)],
    )

    class Meta:
        abstract = True

    @classmethod
    def get_rights(cls, action):
        """
        Recording the daily progress is the project's `timeEntry` business action
        (209006), not an `update` of the project - hence the explicit mapping rather
        than a `scope_parent`. Both subclasses (individual and group) inherit it, the
        right being common to the two as it is at the call site.
        """
        from social_protection.apps import configured_perms

        if action in ("create", "update", "timeEntry"):
            return configured_perms("project", "timeEntry")
        return None

    def _get_enrollment_instance(self):
        """
        Subclasses must override this to return the enrollment object.
        """
        raise NotImplementedError("_get_enrollment_instance() must be implemented in subclass")

    def clean(self):
        enrollment = self._get_enrollment_instance()
        project = enrollment.project

        if not 1 <= self.day_number <= project.working_days:
            raise ValidationError(
                _("Day number must be between 1 and %(working_days)s.") % {"working_days": project.working_days}
            )

        super().clean()

    def __str__(self):
        enrollment = self._get_enrollment_instance()
        return f"{enrollment} - Day {self.day_number}: {self.percent_complete}%"


class BeneficiaryProjectTimeEntry(AbstractProjectTimeEntry):
    enrollment = models.ForeignKey(
        'BeneficiaryProjectEnrollment',
        models.DO_NOTHING,
        related_name='time_entries',
        null=False,
    )

    class Meta:
        unique_together = ('enrollment', 'day_number')
        verbose_name = _("Beneficiary Project Time Entry")
        verbose_name_plural = _("Beneficiary Project Time Entries")

    def _get_enrollment_instance(self):
        return self.enrollment


class GroupBeneficiaryProjectTimeEntry(AbstractProjectTimeEntry):
    enrollment = models.ForeignKey(
        'GroupBeneficiaryProjectEnrollment',
        models.DO_NOTHING,
        related_name='time_entries',
        null=False,
    )

    class Meta:
        unique_together = ('enrollment', 'day_number')
        verbose_name = _("Group Beneficiary Project Time Entry")
        verbose_name_plural = _("Group Beneficiary Project Time Entries")

    def _get_enrollment_instance(self):
        return self.enrollment


# No `get_rights` and no `scope_parent` here: the only call site
# (Query.resolve_beneficiary_data_upload_history) guards this history with
# `gql_beneficiary_search_perms` (170001), whereas the object belongs to the programme.
# Declaring `scope_parent = "benefit_plan"` would change the right enforced (160001);
# the discrepancy is reported to the audit and fixed in another batch of work.
class BenefitPlanDataUploadRecords(core_models.HistoryModel):
    data_upload = models.ForeignKey(IndividualDataSourceUpload, models.DO_NOTHING, null=False)
    benefit_plan = models.ForeignKey(BenefitPlan, models.DO_NOTHING, null=False)
    workflow = models.CharField(max_length=50)

    def __str__(self):
        return f"{self.benefit_plan.code} {self.data_upload.source_name} {self.workflow} {self.date_created}"


class GroupBeneficiary(core_models.HistoryBusinessModel):
    @classmethod
    def get_rights(cls, action):
        """
        Group beneficiaries are guarded by the rights of the `beneficiary` entity
        (170001-170004): that is what Create/Update/DeleteGroupBeneficiaryMutation and
        the group resolvers do, and the catalogue holds neither an identifier nor a
        config key specific to them. The alias is therefore declared here, on the
        model, rather than invented as an entity with no right of its own in
        `DJANGO_PERMS`.

        No `scope_parent = "benefit_plan"`: that would enforce the programme's rights
        (160xxx), which are not the ones the call sites check.
        """
        from social_protection.apps import configured_perms

        return configured_perms("beneficiary", action)

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


class JSONUpdate(Func):
    function = 'JSONB_SET'
    arity = 3
