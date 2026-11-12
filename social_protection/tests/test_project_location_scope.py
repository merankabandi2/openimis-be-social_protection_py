from django.test import TestCase

from core.test_helpers import create_test_interactive_user, create_test_role
from location.test_helpers import assign_user_districts, create_test_village
from social_protection.models import Project
from social_protection.services import ProjectService
from social_protection.tests.test_helpers import (
    find_or_create_activity,
    find_or_create_benefit_plan,
)


class ProjectLocationScopeTest(TestCase):
    """A project is visible to, and editable by, the users of the district its
    location belongs to, whichever level the location is at."""

    @classmethod
    def setUpTestData(cls):
        cls.admin = create_test_interactive_user(username="pScopeAdmin")
        cls.benefit_plan = find_or_create_benefit_plan({"name": "PSCOPEPLAN"}, cls.admin.username)
        cls.activity = find_or_create_activity("Scope Outreach", cls.admin.username)

        village = create_test_village({"code": "PScV1"})
        municipality = village.parent
        cls.district = municipality.parent

        cls.village_project = cls._project("Village project", village)
        cls.municipality_project = cls._project("Municipality project", municipality)
        cls.district_project = cls._project("District project", cls.district)
        cls.other_district_project = cls._project(
            "Other district project", create_test_village({"code": "PScV2"})
        )

        cls.district_user = create_test_interactive_user(
            username="pScopeDistrictUser", roles=[create_test_role().id]
        )
        assign_user_districts(cls.district_user, [cls.district.code])
        cls.user_without_district = create_test_interactive_user(
            username="pScopeNoDistrictUser", roles=[create_test_role().id]
        )

    @classmethod
    def _project(cls, name, location):
        project = Project(
            name=name,
            benefit_plan=cls.benefit_plan,
            activity=cls.activity,
            location=location,
            target_beneficiaries=10,
            working_days=20,
        )
        project.save(username=cls.admin.username)
        return project

    def _visible(self, user):
        return set(Project.get_queryset(Project.objects.all(), user).values_list("id", flat=True))

    def _rename(self, user, project, name):
        result = ProjectService(user).update({"id": project.id, "name": name})
        project.refresh_from_db()
        return result

    def test_district_user_sees_the_projects_of_the_district_at_every_level(self):
        visible = self._visible(self.district_user)

        self.assertIn(self.village_project.id, visible)
        self.assertIn(self.municipality_project.id, visible)
        self.assertIn(self.district_project.id, visible)
        self.assertNotIn(self.other_district_project.id, visible)

    def test_user_without_district_sees_no_project(self):
        visible = self._visible(self.user_without_district)

        self.assertFalse(visible & {
            self.village_project.id,
            self.municipality_project.id,
            self.district_project.id,
            self.other_district_project.id,
        })

    def test_district_user_updates_the_projects_below_the_district(self):
        for project in (self.village_project, self.municipality_project):
            with self.subTest(project=project.name):
                result = self._rename(self.district_user, project, f"{project.name} renamed")

                self.assertTrue(result.get("success"), result)
                self.assertTrue(project.name.endswith(" renamed"))

    def test_district_user_cannot_update_a_project_of_another_district(self):
        result = self._rename(self.district_user, self.other_district_project, "Not allowed")

        self.assertFalse(result.get("success"), result)
        self.assertEqual(self.other_district_project.name, "Other district project")

    def test_user_without_district_cannot_update_a_project(self):
        result = self._rename(self.user_without_district, self.village_project, "Not allowed")

        self.assertFalse(result.get("success"), result)
        self.assertEqual(self.village_project.name, "Village project")
