import json
from core.models import User
from core.models.openimis_graphql_test_case import BaseTestContext
from core.test_helpers import create_test_interactive_user
from social_protection.tests.test_helpers import (
    PatchedOpenIMISGraphQLTestCase,
    find_or_create_activity,
    find_or_create_benefit_plan,
)
from social_protection.models import Project, ProjectMutation
from location.test_helpers import create_test_village
from django.contrib.auth import get_user_model
import uuid


class ProjectsGQLTest(PatchedOpenIMISGraphQLTestCase):

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
        cls.another_activity = find_or_create_activity("Tree Planting", username)
        cls.location = create_test_village({'code': 'ProTV1'})
        cls.another_location = create_test_village({'code': 'ProTV2'})

        cls.project_1 = Project(
            name="Village Health Project A",
            benefit_plan=cls.benefit_plan,
            activity=cls.activity,
            location=cls.location,
            target_beneficiaries=100,
            working_days=120,
        )
        cls.project_1.save(username=username)

        cls.project_2 = Project(
            name="Village Health Project B",
            benefit_plan=cls.benefit_plan,
            activity=cls.activity,
            location=cls.another_location,
            target_beneficiaries=150,
            working_days=90,
        )
        cls.project_2.save(username=username)

        cls.deleted_project = Project(
            name="Deleted Project",
            benefit_plan=cls.benefit_plan,
            activity=cls.activity,
            location=cls.location,
            target_beneficiaries=150,
            working_days=90,
            is_deleted=True,
        )
        cls.deleted_project.save(username=username)

    def test_project_query(self):
        response = self.query(
            """
            query {
              project(first: 10, isDeleted: false) {
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
                    workingDays
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

    def test_project_query_with_location_filter(self):
        query = """
            query($parentLocation: String!) {
              project(parentLocation: $parentLocation, isDeleted: false, first: 10) {
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
        variables = {
            "parentLocation": str(self.location.uuid)
        }

        response = self.query(
            query,
            variables=variables,
            headers={"HTTP_AUTHORIZATION": f"Bearer {self.user_token}"}
        )
        self.assertResponseNoErrors(response)

        content = json.loads(response.content)
        data = content["data"]["project"]

        self.assertEqual(data["totalCount"], 1)
        returned_names = [edge["node"]["name"] for edge in data["edges"]]
        self.assertIn("Village Health Project A", returned_names)
        self.assertNotIn("Village Health Project B", returned_names)

    def test_create_project_mutation_success(self):
        mutation = """
        mutation CreateProject($input: CreateProjectMutationInput!) {
          createProject(input: $input) {
            clientMutationId
            internalId
          }
        }
        """

        project_name = "New Village Sanitation Project"
        variables = {
            "input": {
                "benefitPlanId": str(self.benefit_plan.id),
                "name": project_name,
                "activityId": str(self.activity.id),
                "locationId": str(self.location.uuid),
                "targetBeneficiaries": 200,
                "workingDays": 90,
                "clientMutationId": "abc123"
            }
        }

        response = self.query(
            mutation,
            variables=variables,
            headers={"HTTP_AUTHORIZATION": f"Bearer {self.user_token}"}
        )

        self.assertResponseNoErrors(response)
        data = json.loads(response.content)['data']['createProject']
        self.assert_mutation_success(data['internalId'], self.user_token)
        client_mutation_id = data['clientMutationId']

        # Verify project is created in DB
        project_qs = Project.objects.filter(
            name=project_name,
            benefit_plan=self.benefit_plan,
            activity=self.activity,
            location=self.location,
            target_beneficiaries=200
        )
        self.assertTrue(project_qs.exists())

        # Verify project mutation is created in DB
        project_mutation_exists = ProjectMutation.objects.filter(
            project=project_qs.first(),
            mutation__client_mutation_id=client_mutation_id,
        ).exists()
        self.assertTrue(project_mutation_exists)

        # Verify project can be queried by client_mutation_id
        response = self.query(
            f"""
            query {{
              project(clientMutationId: "{client_mutation_id}") {{
                totalCount
                edges {{
                  node {{
                    id
                    name
                    status
                  }}
                }}
              }}
            }}
            """,
            headers={"HTTP_AUTHORIZATION": f"Bearer {self.user_token}"}
        )
        self.assertResponseNoErrors(response)

        data = json.loads(response.content)['data']['project']
        self.assertEqual(data['totalCount'], 1)

        names_returned = [edge['node']['name'] for edge in data['edges']]
        self.assertIn(project_name, names_returned)

    def test_create_project_mutation_requires_authentication(self):
        mutation = """
        mutation {
          createProject(input: {
            benefitPlanId: "%s",
            name: "Unauthorized Project",
            activityId: "%s",
            locationId: "%s",
            targetBeneficiaries: 80,
            workingDays: 90
          }) {
            clientMutationId
            internalId
          }
        }
        """ % (self.benefit_plan.id, self.activity.id, self.location.uuid)

        response = self.query(mutation)
        self.assertResponseNoErrors(response)

        data = json.loads(response.content)['data']['createProject']
        self.assert_mutation_error(data['internalId'], self.user_token, "authentication_required")


    def test_create_project_mutation_missing_required_field(self):
        mutation = """
        mutation {
          createProject(input: {
            name: "Missing Location",
            benefitPlanId: "%s",
            activityId: "%s",
            targetBeneficiaries: 120,
            workingDays: 90
          }) {
            clientMutationId
            internalId
          }
        }
        """ % (self.benefit_plan.id, self.activity.id)

        response = self.query(
            mutation,
            headers={"HTTP_AUTHORIZATION": f"Bearer {self.user_token}"}
        )
        self.assertEqual(response.status_code, 400)
        content = json.loads(response.content)
        self.assertIn("errors", content)


    def test_update_project_mutation_success(self):
        mutation = """
        mutation UpdateProject($input: UpdateProjectMutationInput!) {
          updateProject(input: $input) {
            clientMutationId
            internalId
          }
        }
        """

        variables = {
            "input": {
                "id": str(self.project_1.id),
                "name": "Updated Village Health Project A",
                "targetBeneficiaries": 120,
                "workingDays": 130,
                "activityId": str(self.another_activity.id),
                "locationId": str(self.another_location.uuid),
                "clientMutationId": "xyz789"
            }
        }

        response = self.query(
            mutation,
            variables=variables,
            headers={"HTTP_AUTHORIZATION": f"Bearer {self.user_token}"}
        )

        self.assertResponseNoErrors(response)
        data = json.loads(response.content)['data']['updateProject']
        self.assert_mutation_success(data['internalId'], self.user_token)

        # Verify project is updated in DB
        updated_project = Project.objects.get(id=self.project_1.id)
        self.assertEqual(updated_project.name, "Updated Village Health Project A")
        self.assertEqual(updated_project.target_beneficiaries, 120)
        self.assertEqual(updated_project.working_days, 130)
        self.assertEqual(updated_project.activity.id, self.another_activity.id)
        self.assertEqual(updated_project.location.id, self.another_location.id)

    def test_update_project_mutation_requires_authentication(self):
        mutation = """
        mutation {
          updateProject(input: {
            id: "%s",
            name: "Unauthorized Update Project",
            targetBeneficiaries: 100,
            workingDays: 90
          }) {
            clientMutationId
            internalId
          }
        }
        """ % self.project_1.id

        response = self.query(mutation)
        self.assertResponseNoErrors(response)

        data = json.loads(response.content)['data']['updateProject']
        self.assert_mutation_error(
            data['internalId'], self.user_token, "authentication_required"
        )

        response = self.query(
            mutation,
            headers={"HTTP_AUTHORIZATION": f"Bearer {self.test_officer_token}"}
        )

        self.assertResponseNoErrors(response)
        content = json.loads(response.content)
        data = content['data']['updateProject']
        self.assert_mutation_error(
            data['internalId'], self.user_token, "authentication_required"
        )

    def test_delete_project_mutation_success(self):
        mutation = """
        mutation DeleteProject($input: DeleteProjectMutationInput!) {
          deleteProject(input: $input) {
            clientMutationId
            internalId
          }
        }
        """

        variables = {
            "input": {
                "ids": [str(self.project_1.id), str(self.project_2.id)],
                "clientMutationId": "delete123"
            }
        }

        response = self.query(
            mutation,
            variables=variables,
            headers={"HTTP_AUTHORIZATION": f"Bearer {self.user_token}"}
        )

        self.assertResponseNoErrors(response)
        data = json.loads(response.content)['data']['deleteProject']
        self.assert_mutation_success(data['internalId'], self.user_token)

    def test_delete_project_mutation_requires_authentication(self):
        mutation = """
        mutation {
          deleteProject(input: {
            ids: ["%s", "%s"]
          }) {
            clientMutationId
            internalId
          }
        }
        """ % (self.project_1.id, self.project_2.id)

        response = self.query(mutation)
        self.assertResponseNoErrors(response)

        data = json.loads(response.content)['data']['deleteProject']
        self.assert_mutation_error(data['internalId'], self.user_token, 'authentication_required')

        response = self.query(
            mutation,
            headers={"HTTP_AUTHORIZATION": f"Bearer {self.test_officer_token}"}
        )

        self.assertResponseNoErrors(response)
        data = json.loads(response.content)['data']['deleteProject']
        self.assert_mutation_error(data['internalId'], self.user_token, 'authentication_required')

    def test_undo_delete_project_mutation_success(self):
        undo_mutation = """
        mutation UndoDeleteProject($input: UndoDeleteProjectMutationInput!) {
          undoDeleteProject(input: $input) {
            clientMutationId
            internalId
          }
        }
        """

        # Undo the delete for the same projects
        undo_variables = {
            "input": {
                "ids": [str(self.deleted_project.id)],
                "clientMutationId": "undo123"
            }
        }

        response = self.query(
            undo_mutation,
            variables=undo_variables,
            headers={"HTTP_AUTHORIZATION": f"Bearer {self.user_token}"}
        )

        self.assertResponseNoErrors(response)
        data = json.loads(response.content)['data']['undoDeleteProject']
        self.assert_mutation_success(data['internalId'], self.user_token)

    def test_undo_delete_project_mutation_requires_authentication(self):
        undo_mutation = """
        mutation {
          undoDeleteProject(input: {
            ids: ["%s"]
          }) {
            clientMutationId
            internalId
          }
        }
        """ % (self.deleted_project.id)

        # Test for unauthenticated user
        response = self.query(undo_mutation)
        self.assertResponseNoErrors(response)

        data = json.loads(response.content)['data']['undoDeleteProject']
        self.assert_mutation_error(data['internalId'], self.user_token, 'authentication_required')

        # Test for user without permission (test_officer)
        response = self.query(
            undo_mutation,
            headers={"HTTP_AUTHORIZATION": f"Bearer {self.test_officer_token}"}
        )

        self.assertResponseNoErrors(response)
        data = json.loads(response.content)['data']['undoDeleteProject']
        self.assert_mutation_error(data['internalId'], self.test_officer_token, 'authentication_required')

    def test_undo_delete_project_mutation_invalid_ids(self):
        undo_mutation = """
        mutation UndoDeleteProject($input: UndoDeleteProjectMutationInput!) {
          undoDeleteProject(input: $input) {
            clientMutationId
            internalId
          }
        }
        """

        undo_variables = {
            "input": {
                "ids": [str(uuid.uuid4())],
                "clientMutationId": "undo123"
            }
        }

        response = self.query(
            undo_mutation,
            variables=undo_variables,
            headers={"HTTP_AUTHORIZATION": f"Bearer {self.user_token}"}
        )

        self.assertResponseNoErrors(response)
        data = json.loads(response.content)['data']['undoDeleteProject']
        self.assert_mutation_error(data['internalId'], self.user_token, "does not exist")

    def test_project_history_query_success(self):
        query = """
        query {
          projectHistory(first: 10) {
            totalCount
            edges {
              node {
                uuid
                name
                status
                isDeleted
                version
              }
            }
          }
        }
        """
        response = self.query(
            query,
            headers={"HTTP_AUTHORIZATION": f"Bearer {self.user_token}"}
        )
        self.assertResponseNoErrors(response)
        content = json.loads(response.content)["data"]["projectHistory"]
        self.assertGreaterEqual(content["totalCount"], 1)
        names = [edge["node"]["name"] for edge in content["edges"]]
        self.assertIn("Village Health Project A", names)

    def test_project_history_query_filter_is_deleted(self):
        query = """
        query {
          projectHistory(isDeleted: true, first: 10) {
            edges {
              node {
                name
                isDeleted
              }
            }
          }
        }
        """
        response = self.query(
            query,
            headers={"HTTP_AUTHORIZATION": f"Bearer {self.user_token}"}
        )
        self.assertResponseNoErrors(response)
        edges = json.loads(response.content)["data"]["projectHistory"]["edges"]
        self.assertTrue(any(e["node"]["name"] == "Deleted Project" for e in edges))
        self.assertTrue(all(e["node"]["isDeleted"] for e in edges))

    def test_project_history_query_unauthorized(self):
        query = """
        query {
          projectHistory(first: 5) {
            totalCount
          }
        }
        """
        # Anonymous
        response = self.query(query)
        content = json.loads(response.content)
        self.assertEqual(content['errors'][0]['message'], 'Unauthorized')

        # Unprivileged user
        response = self.query(
            query,
            headers={"HTTP_AUTHORIZATION": f"Bearer {self.test_officer_token}"}
        )
        content = json.loads(response.content)
        self.assertEqual(content['errors'][0]['message'], 'Unauthorized')

    def test_project_history_query_search(self):
        query = """
        query {
          projectHistory(search: "Health A", first: 10) {
            edges {
              node {
                name
              }
            }
          }
        }
        """
        response = self.query(
            query,
            headers={"HTTP_AUTHORIZATION": f"Bearer {self.user_token}"}
        )
        self.assertResponseNoErrors(response)
        edges = json.loads(response.content)["data"]["projectHistory"]["edges"]
        names = [e["node"]["name"] for e in edges]
        self.assertTrue(any("Village Health Project A" in name for name in names))

    def test_project_history_query_sort_alphabetically(self):
        query = """
        query {
          projectHistory(sortAlphabetically: true, first: 10) {
            edges {
              node {
                name
              }
            }
          }
        }
        """
        response = self.query(
            query,
            headers={"HTTP_AUTHORIZATION": f"Bearer {self.user_token}"}
        )
        self.assertResponseNoErrors(response)
        edges = json.loads(response.content)["data"]["projectHistory"]["edges"]
        names = [e["node"]["name"] for e in edges]
        self.assertEqual(names, sorted(names))
