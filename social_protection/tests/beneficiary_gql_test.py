from unittest import mock
import graphene
from core.models import User
from core.models.openimis_graphql_test_case import BaseTestContext
from core.test_helpers import create_test_interactive_user
from social_protection import schema as sp_schema
from graphene import Schema
from graphene.test import Client
from graphene_django.utils.testing import GraphQLTestCase
from django.conf import settings
from graphql_jwt.shortcuts import get_token
from social_protection.tests.test_helpers import (
    PatchedOpenIMISGraphQLTestCase,
    create_benefit_plan,
    create_individual,
    add_individual_to_benefit_plan,
    create_project,
)
from social_protection.services import BeneficiaryService
from location.test_helpers import create_test_village
import json

class BeneficiaryGQLTest(PatchedOpenIMISGraphQLTestCase):
    schema = Schema(query=sp_schema.Query)

    class AnonymousUserContext:
        user = mock.Mock(is_anonymous=True)

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.user = User.objects.filter(username='admin', i_user__isnull=False).first()
        if not cls.user:
            cls.user=create_test_interactive_user(username='admin')
        # some test data so as to created contract properly
        cls.user_token = BaseTestContext(user=cls.user).get_jwt()
        cls.benefit_plan = create_benefit_plan(cls.user.username, payload_override={
            'code': 'SGQLTest',
            'type': "INDIVIDUAL"
        })
        cls.individual_2child = create_individual(cls.user.username)
        cls.individual_1child = create_individual(cls.user.username, payload_override={
            'first_name': 'OneChild',
            'json_ext': {
                'number_of_children': 1
            }
        })
        cls.individual =  create_individual(cls.user.username, payload_override={
            'first_name': 'NoChild',
            'json_ext': {
                'number_of_children': 0
            }
        })
        cls.individual_not_enrolled =  create_individual(cls.user.username, payload_override={
            'first_name': 'Not enrolled',
            'json_ext': {
                'number_of_children': 0,
                'able_bodied': True
            }
        })
        cls.service = BeneficiaryService(cls.user)

        add_individual_to_benefit_plan(cls.service, cls.individual_2child, cls.benefit_plan)
        add_individual_to_benefit_plan(cls.service, cls.individual_1child, cls.benefit_plan)
        add_individual_to_benefit_plan(cls.service, cls.individual,
                                       cls.benefit_plan, payload_override={'status': 'ACTIVE'})

    def test_query_beneficiary_basic(self):
        response = self.query(
            f"""
            query {{
              beneficiary(benefitPlan_Id: "{self.benefit_plan.uuid}", isDeleted: false, first: 10) {{
                totalCount
                pageInfo {{
                  hasNextPage
                  hasPreviousPage
                  startCursor
                  endCursor
                }}
                edges {{
                  node {{
                    id
                    jsonExt
                    benefitPlan {{
                      id
                    }}
                    individual {{
                      firstName
                      lastName
                      dob
                    }}
                    status
                    isEligible
                  }}
                }}
              }}
            }}
            """
        , headers={"HTTP_AUTHORIZATION": f"Bearer {self.user_token}"})
        self.assertResponseNoErrors(response)
        response_data = json.loads(response.content)

        # Asserting the response has one beneficiary record
        beneficiary_data = response_data['data']['beneficiary']
        self.assertEqual(beneficiary_data['totalCount'], 3)

        enrolled_first_names = list(
            e['node']['individual']['firstName'] for e in beneficiary_data['edges']
        )
        self.assertTrue(self.individual.first_name in enrolled_first_names)
        self.assertTrue(self.individual_1child.first_name in enrolled_first_names)
        self.assertTrue(self.individual_2child.first_name in enrolled_first_names)
        self.assertFalse(self.individual_not_enrolled.first_name in enrolled_first_names)

        # eligibility is status specific, so None is expected for all records without status filter
        eligible_none = list(
            e['node']['isEligible'] is None for e in beneficiary_data['edges']
        )
        self.assertTrue(all(eligible_none))


    def test_query_beneficiary_individual_filter(self):
        query_str = f"""
            query {{
              beneficiary(
                benefitPlan_Id: "{self.benefit_plan.uuid}",
                individual_FirstName_Icontains: "no",
                first: 10
              ) {{
                totalCount
                pageInfo {{
                  hasNextPage
                  hasPreviousPage
                  startCursor
                  endCursor
                }}
                edges {{
                  node {{
                    id
                    jsonExt
                    benefitPlan {{
                      id
                    }}
                    individual {{
                      firstName
                      lastName
                      dob
                    }}
                    status
                  }}
                }}
              }}
            }}
        """
        response = self.query(query_str,
                              headers={"HTTP_AUTHORIZATION": f"Bearer {self.user_token}"})
        self.assertResponseNoErrors(response)
        response_data = json.loads(response.content)

        beneficiary_data = response_data['data']['beneficiary']
        self.assertEqual(beneficiary_data['totalCount'], 1)

        self.assertEqual(
            beneficiary_data['edges'][0]['node']['individual']['firstName'],
            self.individual.first_name
        )


    def test_query_beneficiary_custom_filter(self):
        query_str = f"""
            query {{
              beneficiary(
                benefitPlan_Id: "{self.benefit_plan.uuid}",
                customFilters: ["number_of_children__lt__integer=2"],
                isDeleted: false,
                first: 10
              ) {{
                totalCount
                pageInfo {{
                  hasNextPage
                  hasPreviousPage
                  startCursor
                  endCursor
                }}
                edges {{
                  node {{
                    id
                    jsonExt
                    benefitPlan {{
                      id
                    }}
                    individual {{
                      firstName
                      lastName
                      dob
                    }}
                    status
                  }}
                }}
              }}
            }}
        """
        response = self.query(query_str,
                              headers={"HTTP_AUTHORIZATION": f"Bearer {self.user_token}"})
        self.assertResponseNoErrors(response)
        response_data = json.loads(response.content)

        beneficiary_data = response_data['data']['beneficiary']
        self.assertEqual(beneficiary_data['totalCount'], 2)

        returned_first_names = list(
            e['node']['individual']['firstName'] for e in beneficiary_data['edges']
        )
        self.assertTrue(self.individual.first_name in returned_first_names)
        self.assertTrue(self.individual_1child.first_name in returned_first_names)
        self.assertFalse(self.individual_2child.first_name in returned_first_names)

        query_str = query_str.replace('__lt__', '__gte__')

        response = self.query(query_str,
                              headers={"HTTP_AUTHORIZATION": f"Bearer {self.user_token}"})
        self.assertResponseNoErrors(response)
        response_data = json.loads(response.content)

        beneficiary_data = response_data['data']['beneficiary']
        self.assertEqual(beneficiary_data['totalCount'], 1)

        beneficiary_node = beneficiary_data['edges'][0]['node']
        individual_data = beneficiary_node['individual']
        self.assertEqual(individual_data['firstName'], self.individual_2child.first_name)


    def test_query_beneficiary_status_filter(self):
        query_str = f"""
            query {{
              beneficiary(
                benefitPlan_Id: "{self.benefit_plan.uuid}",
                status: POTENTIAL,
                isDeleted: false,
                first: 10
              ) {{
                totalCount
                pageInfo {{
                  hasNextPage
                  hasPreviousPage
                  startCursor
                  endCursor
                }}
                edges {{
                  node {{
                    id
                    jsonExt
                    benefitPlan {{
                      id
                    }}
                    individual {{
                      firstName
                      lastName
                      dob
                    }}
                    status
                    isEligible
                  }}
                }}
              }}
            }}
        """
        response = self.query(query_str,
                              headers={"HTTP_AUTHORIZATION": f"Bearer {self.user_token}"})
        self.assertResponseNoErrors(response)
        response_data = json.loads(response.content)

        beneficiary_data = response_data['data']['beneficiary']
        self.assertEqual(beneficiary_data['totalCount'], 2)

        enrolled_first_names = list(
            e['node']['individual']['firstName'] for e in beneficiary_data['edges']
        )
        self.assertFalse(self.individual.first_name in enrolled_first_names)
        self.assertTrue(self.individual_1child.first_name in enrolled_first_names)
        self.assertTrue(self.individual_2child.first_name in enrolled_first_names)
        self.assertFalse(self.individual_not_enrolled.first_name in enrolled_first_names)

        def find_beneficiary_by_first_name(first_name):
            for edge in beneficiary_data['edges']:
                if edge['node']['individual']['firstName'] == first_name:
                    return edge['node']
            return None

        beneficiary_1child = find_beneficiary_by_first_name(self.individual_1child.first_name)
        self.assertFalse(beneficiary_1child['isEligible'])

        beneficiary_2child = find_beneficiary_by_first_name(self.individual_2child.first_name)
        self.assertTrue(beneficiary_2child['isEligible'])


    def test_query_beneficiary_eligibility_filter(self):
        query_str = f"""
            query {{
              beneficiary(
                benefitPlan_Id: "{self.benefit_plan.uuid}",
                status: POTENTIAL,
                isEligible: true,
                isDeleted: false,
                first: 10
              ) {{
                totalCount
                pageInfo {{
                  hasNextPage
                  hasPreviousPage
                  startCursor
                  endCursor
                }}
                edges {{
                  node {{
                    id
                    jsonExt
                    benefitPlan {{
                      id
                    }}
                    individual {{
                      firstName
                      lastName
                      dob
                    }}
                    status
                    isEligible
                  }}
                }}
              }}
            }}
        """
        response = self.query(query_str,
                              headers={"HTTP_AUTHORIZATION": f"Bearer {self.user_token}"})
        self.assertResponseNoErrors(response)
        response_data = json.loads(response.content)

        beneficiary_data = response_data['data']['beneficiary']
        self.assertEqual(beneficiary_data['totalCount'], 1)

        enrolled_first_names = list(
            e['node']['individual']['firstName'] for e in beneficiary_data['edges']
        )
        self.assertFalse(self.individual_1child.first_name in enrolled_first_names)
        self.assertTrue(self.individual_2child.first_name in enrolled_first_names)

        eligible = list(
            e['node']['isEligible'] for e in beneficiary_data['edges']
        )
        self.assertTrue(all(eligible))

    def test_query_beneficiary_project_filter(self):
        # Enroll self.individual to a project
        project = create_project(
            'test enrollment project',
            self.benefit_plan,
            self.user.username,
        )

        # Link the project to the ACTIVE beneficiary
        self.individual.beneficiary_set.filter(benefit_plan=self.benefit_plan).update(project=project)

        # Query with projectId filter
        query_str = f"""
            query {{
              beneficiary(
                project_Id: "{project.uuid}",
                isDeleted: false,
                first: 10
              ) {{
                totalCount
                edges {{
                  node {{
                    id
                    individual {{
                      firstName
                    }}
                    project {{
                      id
                      name
                    }}
                    status
                  }}
                }}
              }}
            }}
        """
        response = self.query(query_str, headers={"HTTP_AUTHORIZATION": f"Bearer {self.user_token}"})
        self.assertResponseNoErrors(response)
        response_data = json.loads(response.content)

        beneficiary_data = response_data['data']['beneficiary']
        self.assertEqual(beneficiary_data['totalCount'], 1)

        returned_node = beneficiary_data['edges'][0]['node']
        self.assertEqual(returned_node['individual']['firstName'], self.individual.first_name)
        self.assertEqual(returned_node['status'], 'ACTIVE')
        self.assertEqual(returned_node['project']['name'], project.name)

    def test_query_beneficiary_village_or_child_of_filter(self):
        child_village = create_test_village({'code': 'BeneV1', 'name': 'Beneficiary Village 1'})
        parent_location = child_village.parent.parent

        # Create a new individual in the test village and enroll them
        village_individual = create_individual(self.user.username, payload_override={
            "first_name": "VillagePerson",
            "location_id": child_village.id,
        })
        add_individual_to_benefit_plan(self.service, village_individual, self.benefit_plan)

        # Create a control individual elsewhere
        another_village = create_test_village({'code': 'BeneV2', 'name': 'Beneficiary Village 2'})
        other_individual = create_individual(self.user.username, payload_override={
            "first_name": "OtherPerson",
            "location_id": another_village.id,
        })
        add_individual_to_benefit_plan(self.service, other_individual, self.benefit_plan)

        # Run the query with villageOrChildOf = parent district ID
        query_str = f"""
        query {{
          beneficiary(
            benefitPlan_Id: "{self.benefit_plan.uuid}",
            villageOrChildOf: {parent_location.id},
            isDeleted: false,
            first: 10
          ) {{
            totalCount
            edges {{
              node {{
                individual {{
                  firstName
                }}
              }}
            }}
          }}
        }}
        """
        response = self.query(query_str, headers={"HTTP_AUTHORIZATION": f"Bearer {self.user_token}"})
        self.assertResponseNoErrors(response)
        data = json.loads(response.content)['data']['beneficiary']

        self.assertEqual(data['totalCount'], 1)
        self.assertEqual(data['edges'][0]['node']['individual']['firstName'], "VillagePerson")

    def test_project_beneficiary_enrollment(self):
        project = create_project(
            'test enrollment project',
            self.benefit_plan,
            self.user.username,
        )

        query_str = f'''
            mutation {{
              enrollProject(
                input: {{
                  ids: ["{self.individual_1child.id}", "{self.individual_2child.id}"]
                  projectId: "{str(project.id)}"
                }}
              ) {{
                clientMutationId
                internalId
              }}
            }}
        '''
        response = self.query(
            query_str,
            headers={"HTTP_AUTHORIZATION": f"Bearer {self.user_token}"}
        )
        self.assertResponseNoErrors(response)

        content = json.loads(response.content)
        internal_id = content['data']['enrollProject']['internalId']
        self.assert_mutation_success(internal_id, self.user_token)

    def test_query_beneficiary_search(self):
        # search matches on first name
        village = create_test_village({'code': 'SearchV', 'name': 'SearchVillage'})
        search_individual = create_individual(self.user.username, payload_override={
            "first_name": "SearchMatch",
            "location_id": village.id,
        })
        add_individual_to_benefit_plan(self.service, search_individual, self.benefit_plan)

        # search matches on location name
        child_village = create_test_village({'code': 'BeneV1', 'name': 'Village Match'})
        village_individual = create_individual(self.user.username, payload_override={
            "first_name": "VillagePerson",
            "location_id": child_village.id,
        })
        add_individual_to_benefit_plan(self.service, village_individual, self.benefit_plan)

        # search matches on json ext field value
        ext_individual = create_individual(self.user.username, payload_override={
            "first_name": "JsonExtPerson",
            'json_ext': {
                'abc': 'json mAtch here',
            }
        })
        add_individual_to_benefit_plan(self.service, ext_individual, self.benefit_plan)

        response = self.query(
            f"""
            query {{
              beneficiary(
                benefitPlan_Id: "{self.benefit_plan.uuid}",
                search: "match",
                isDeleted: false,
                first: 10
              ) {{
                totalCount
                edges {{
                  node {{
                    individual {{
                      firstName
                    }}
                  }}
                }}
              }}
            }}
            """,
            headers={"HTTP_AUTHORIZATION": f"Bearer {self.user_token}"}
        )

        self.assertResponseNoErrors(response)
        data = json.loads(response.content)['data']['beneficiary']

        first_names = list(
            e['node']['individual']['firstName'] for e in data['edges']
        )
        self.assertTrue(search_individual.first_name in first_names)
        self.assertTrue(village_individual.first_name in first_names)
        self.assertTrue(ext_individual.first_name in first_names)
        self.assertEqual(data['totalCount'], 3)

    def test_query_beneficiary_filter_location(self):
        village = create_test_village({'code': 'LocV1', 'name': 'FfBLV'})
        district_name_partial = village.parent.parent.name.lower()[-5:]
        location_individual = create_individual(self.user.username, payload_override={
            "first_name": "LocationPerson",
            "location_id": village.id,
        })
        add_individual_to_benefit_plan(self.service, location_individual, self.benefit_plan)

        another_village = create_test_village({'code': 'LocV2', 'name': 'XXZV'})
        another_individual = create_individual(self.user.username, payload_override={
            "first_name": "AnotherPerson",
            "location_id": another_village.id,
        })
        add_individual_to_benefit_plan(self.service, another_individual, self.benefit_plan)

        response = self.query(
            f"""
            query {{
              beneficiary(
                benefitPlan_Id: "{self.benefit_plan.uuid}",
                location: "1:{district_name_partial}",
                isDeleted: false,
                first: 10
              ) {{
                totalCount
                edges {{
                  node {{
                    individual {{
                      firstName
                    }}
                  }}
                }}
              }}
            }}
            """,
            headers={"HTTP_AUTHORIZATION": f"Bearer {self.user_token}"}
        )

        self.assertResponseNoErrors(response)
        data = json.loads(response.content)['data']['beneficiary']

        first_names = list(
            e['node']['individual']['firstName'] for e in data['edges']
        )
        self.assertTrue(location_individual.first_name in first_names)
        self.assertTrue(another_individual.first_name not in first_names)

        # update the query to look for "district" which would return both individuals
        response = self.query(
            f"""
            query {{
              beneficiary(
                benefitPlan_Id: "{self.benefit_plan.uuid}",
                location: "1:disTrict",
                isDeleted: false,
                first: 10
              ) {{
                totalCount
                edges {{
                  node {{
                    individual {{
                      firstName
                    }}
                  }}
                }}
              }}
            }}
            """,
            headers={"HTTP_AUTHORIZATION": f"Bearer {self.user_token}"}
        )

        self.assertResponseNoErrors(response)
        data = json.loads(response.content)['data']['beneficiary']

        first_names = list(
            e['node']['individual']['firstName'] for e in data['edges']
        )
        self.assertTrue(location_individual.first_name in first_names)
        self.assertTrue(another_individual.first_name in first_names)

    def test_query_beneficiary_allows_multiple_enrollments_filter(self):
        # Create two projects, one that allows multiple enrollments, one that doesn't
        multi_project = create_project(
            'MultiProject',
            self.benefit_plan,
            self.user.username,
            allows_multiple_enrollments=True,
        )
        exclusive_project = create_project(
            'ExclusiveProject',
            self.benefit_plan,
            self.user.username,
            allows_multiple_enrollments=False,
        )

        # Assign self.individual_2child to the exclusive project
        self.individual_2child.beneficiary_set\
            .filter(benefit_plan=self.benefit_plan)\
            .update(project=exclusive_project)

        # Assign self.individual_1child to the multi project
        self.individual_1child.beneficiary_set\
            .filter(benefit_plan=self.benefit_plan)\
            .update(project=multi_project)

        # Query using multi-enrollment project filter — should exclude 2child,
        # include 1child & no-project
        query_str = f"""
            query {{
              beneficiary(
                benefitPlan_Id: "{self.benefit_plan.uuid}",
                projectAllowsMultipleEnrollments: "{multi_project.id}",
                isDeleted: false,
                first: 10
              ) {{
                totalCount
                edges {{
                  node {{
                    individual {{
                      firstName
                    }}
                  }}
                }}
              }}
            }}
        """
        response = self.query(query_str, headers={"HTTP_AUTHORIZATION": f"Bearer {self.user_token}"})
        self.assertResponseNoErrors(response)
        data = json.loads(response.content)['data']['beneficiary']
        returned_names = [e['node']['individual']['firstName'] for e in data['edges']]

        self.assertIn(self.individual_1child.first_name, returned_names)  # already enrolled in this multi project
        self.assertIn(self.individual.first_name, returned_names)         # not enrolled in any project
        self.assertNotIn(self.individual_2child.first_name, returned_names)  # enrolled in exclusive project

        # Query using exclusive project — should include only itself or unassigned
        query_str = f"""
            query {{
              beneficiary(
                benefitPlan_Id: "{self.benefit_plan.uuid}",
                projectAllowsMultipleEnrollments: "{exclusive_project.id}",
                isDeleted: false,
                first: 10
              ) {{
                totalCount
                edges {{
                  node {{
                    individual {{
                      firstName
                    }}
                  }}
                }}
              }}
            }}
        """
        response = self.query(query_str, headers={"HTTP_AUTHORIZATION": f"Bearer {self.user_token}"})
        self.assertResponseNoErrors(response)
        data = json.loads(response.content)['data']['beneficiary']
        returned_names = [e['node']['individual']['firstName'] for e in data['edges']]

        self.assertIn(self.individual_2child.first_name, returned_names)  # already enrolled in this project
        self.assertIn(self.individual.first_name, returned_names)         # not enrolled in any project
        self.assertNotIn(self.individual_1child.first_name, returned_names)  # enrolled in a different project
