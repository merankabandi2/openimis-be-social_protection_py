import json
from core.models import User
from core.models.openimis_graphql_test_case import openIMISGraphQLTestCase, BaseTestContext
from core.test_helpers import create_test_interactive_user
from social_protection.tests.test_helpers import (
    find_or_create_activity,
    find_or_create_benefit_plan,
)
from social_protection.models import Project
from location.test_helpers import create_test_village
from django.contrib.auth import get_user_model


class ProjectsGQLTest(openIMISGraphQLTestCase):

    @classmethod
    def setUpTestData(cls):
        cls.user = User.objects.filter(username='admin', i_user__isnull=False).first()
        if not cls.user:
            cls.user = create_test_interactive_user(username='admin')
        cls.user_token = BaseTestContext(user=cls.user).get_jwt()
        username = cls.user.username

        cls.test_officer = create_test_interactive_user(
            username="projectUserNoRight", roles=[1])  # 1 is a generic role with no project access
        cls.test_officer_token = BaseTestContext(user=cls.test_officer).get_jwt()

        # Required dependencies
        cls.benefit_plan = find_or_create_benefit_plan({"name": "TESTPLAN"}, username)
        cls.activity = find_or_create_activity("Community Outreach", username)
        cls.location = create_test_village()

        cls.project_1 = Project(
            name="Village Health Project A",
            benefit_plan=cls.benefit_plan,
            activity=cls.activity,
            location=cls.location,
            target_beneficiaries=100,
        )
        cls.project_1.save(username=username)

        cls.project_2 = Project(
            name="Village Health Project B",
            benefit_plan=cls.benefit_plan,
            activity=cls.activity,
            location=cls.location,
            target_beneficiaries=150,
        )
        cls.project_2.save(username=username)

    def test_project_query(self):
        response = self.query(
            """
            query {
              project(first: 10) {
                totalCount
                edges {
                  node {
                    id
                    name
                    status
                    benefitPlan { name }
                    activity { name }
                    location { name }
                    targetBeneficiaries
                  }
                }
              }
            }
            """,
            headers={"HTTP_AUTHORIZATION": f"Bearer {self.user_token}"}
        )
        self.assertResponseNoErrors(response)

        data = json.loads(response.content)['data']['project']
        self.assertEqual(data['totalCount'], 2)

        names_returned = [edge['node']['name'] for edge in data['edges']]
        self.assertIn("Village Health Project A", names_returned)
        self.assertIn("Village Health Project B", names_returned)

    def test_project_query_permission(self):
        query_str = """
            query {
              project(first: 10) {
                totalCount
                edges {
                  node {
                    id
                    name
                  }
                }
              }
            }
        """

        # Anonymous user
        response = self.query(query_str)
        content = json.loads(response.content)
        self.assertEqual(content['errors'][0]['message'], 'Unauthorized')

        # Unprivileged user
        response = self.query(
            query_str,
            headers={"HTTP_AUTHORIZATION": f"Bearer {self.test_officer_token}"}
        )
        content = json.loads(response.content)
        self.assertEqual(content['errors'][0]['message'], 'Unauthorized')

