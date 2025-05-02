from django.test import TestCase
from django.utils.translation import override
from social_protection.validation import validate_project_unique_name
from core.test_helpers import create_test_interactive_user
from social_protection.tests.test_helpers import create_benefit_plan, create_project
from social_protection.models import Project

class TestValidateProjectUniqueName(TestCase):

    @classmethod
    def setUpTestData(cls):
        user = create_test_interactive_user(username="admin")
        username = user.username

        cls.benefit_plan = create_benefit_plan(username=username)
        cls.other_benefit_plan = create_benefit_plan(
            username=username, payload_override={'code': 'ABCBEN'}
        )
        cls.project = create_project("Duplicate Name", cls.benefit_plan, username)
        cls.deleted_project = create_project("Deleted Name", cls.benefit_plan, username)
        cls.deleted_project.is_deleted = True
        cls.deleted_project.save(username=username)

    def test_returns_error_if_name_already_exists(self):
        with override("en"):
            errors = validate_project_unique_name(self.project.name, self.benefit_plan.id)
            self.assertEqual(len(errors), 1)
            self.assertIn("name_exists", errors[0]["message"])

    def test_returns_empty_if_name_is_unique(self):
        errors = validate_project_unique_name("Unique Name", self.benefit_plan.id)
        self.assertEqual(len(errors), 0)

    def test_ignores_deleted_projects(self):
        errors = validate_project_unique_name(self.deleted_project.name, self.benefit_plan.id)
        self.assertEqual(len(errors), 0)

    def test_returns_empty_if_different_benefit_plan(self):
        errors = validate_project_unique_name(self.project.name, self.other_benefit_plan.id)
        self.assertEqual(len(errors), 0)

    def test_excludes_current_instance_on_edit(self):
        errors = validate_project_unique_name(
            self.project.name, self.benefit_plan.id, uuid=self.project.id
        )
        self.assertEqual(len(errors), 0)

