import json
from core.models import User
from core.models.openimis_graphql_test_case import openIMISGraphQLTestCase, BaseTestContext
from core.test_helpers import create_test_interactive_user
from social_protection.tests.test_helpers import create_activity
from django.contrib.auth import get_user_model


class ActivitiesGQLTest(openIMISGraphQLTestCase):

    @classmethod
    def setUpTestData(cls):
        cls.user = User.objects.filter(username='admin', i_user__isnull=False).first()
        if not cls.user:
            cls.user=create_test_interactive_user(username='admin')
        cls.user_token = BaseTestContext(user=cls.user).get_jwt()
        username = cls.user.username

        cls.med_enroll_officer = create_test_interactive_user(
            username="medEONoRight", roles=[1]) # 1 is the med enrollment officer role
        cls.med_enroll_officer_token = BaseTestContext(user=cls.med_enroll_officer).get_jwt()

        cls.activity_1 = create_activity("Nutrition Outreach", username)
        cls.activity_2 = create_activity("School Enrollment Drive", username)
        cls.activity_3 = create_activity("Public Health Training", username)

    def test_activity_query(self):
        response = self.query(
            """
            query {
              activity(first: 10) {
                totalCount
                edges {
                  node {
                    id
                    name
                  }
                }
              }
            }
            """,
            headers={"HTTP_AUTHORIZATION": f"Bearer {self.user_token}"}
        )
        self.assertResponseNoErrors(response)

        data = json.loads(response.content)['data']['activity']
        self.assertEqual(data['totalCount'], 3)

        names_returned = [edge['node']['name'] for edge in data['edges']]
        self.assertIn("Nutrition Outreach", names_returned)
        self.assertIn("School Enrollment Drive", names_returned)
        self.assertIn("Public Health Training", names_returned)

    def test_activity_query_permission(self):
        query_str = '''
            query {
              activity(first: 10) {
                totalCount
                edges {
                  node {
                    id
                    name
                  }
                }
              }
            }
        '''

        # Anonymous User sees nothing
        response = self.query(query_str)

        content = json.loads(response.content)
        self.assertEqual(content['errors'][0]['message'], 'Unauthorized')

        # Health Enrollment Officier (role=1) sees nothing
        response = self.query(
            query_str,
            headers={"HTTP_AUTHORIZATION": f"Bearer {self.med_enroll_officer_token}"}
        )
        content = json.loads(response.content)
        self.assertEqual(content['errors'][0]['message'], 'Unauthorized')



