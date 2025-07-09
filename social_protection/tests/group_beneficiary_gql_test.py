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
    create_group_with_individual,
    add_group_to_benefit_plan,
    create_individual,
    add_individual_to_group,
    create_project,
)
from social_protection.services import GroupBeneficiaryService
from location.test_helpers import create_test_village
import json

class GroupBeneficiaryGQLTest(PatchedOpenIMISGraphQLTestCase):
    schema = Schema(query=sp_schema.Query)


    class AnonymousUserContext:
        user = mock.Mock(is_anonymous=True)

    @classmethod
    def setUpClass(cls):
        super(GroupBeneficiaryGQLTest, cls).setUpClass()
        cls.user = User.objects.filter(username='admin', i_user__isnull=False).first()
        if not cls.user:
            cls.user = create_test_interactive_user(username='admin')
        cls.user_token = BaseTestContext(user=cls.user).get_jwt()
        cls.benefit_plan = create_benefit_plan(cls.user.username, payload_override={
            'code': 'GGQLTest',
            'type': 'GROUP'
        })
        cls.individual_2child, cls.group_2child, gi = create_group_with_individual(cls.user.username)
        child1 = create_individual(cls.user.username, payload_override={
            'first_name': 'Child1',
            'json_ext': {
                'number_of_children': 0
            }
        })
        child2 = create_individual(cls.user.username, payload_override={
            'first_name': 'Child2',
            'json_ext': {
                'number_of_children': 0
            }
        })
        add_individual_to_group(cls.user.username, child1, cls.group_2child)
        add_individual_to_group(cls.user.username, child2, cls.group_2child)

        cls.individual_1child, cls.group_1child, _ = create_group_with_individual(
            cls.user.username,
            individual_override={
                'first_name': 'OneChild',
                'json_ext': {
                    'number_of_children': 1
                }
            }
        )
        cls.individual, cls.group_0child, _ =  create_group_with_individual(
            cls.user.username,
            individual_override={
                'first_name': 'NoChild',
                'json_ext': {
                    'number_of_children': 0
                }
            }
        )
        cls.individual_not_enrolled, cls.group_not_enrolled, _ =  create_group_with_individual(
            cls.user.username,
            individual_override={
                'first_name': 'Not enrolled',
                'json_ext': {
                    'number_of_children': 0,
                    'able_bodied': True
                }
            }
        )
        cls.service = GroupBeneficiaryService(cls.user)
        add_group_to_benefit_plan(cls.service, cls.group_2child, cls.benefit_plan)
        add_group_to_benefit_plan(cls.service, cls.group_1child, cls.benefit_plan)
        add_group_to_benefit_plan(cls.service, cls.group_0child, cls.benefit_plan,
                                  payload_override={'status': 'ACTIVE'})

    def test_query_beneficiary_basic(self):
        response = self.query(
            f"""
            query {{
              groupBeneficiary(benefitPlan_Id: "{self.benefit_plan.uuid}", isDeleted: false, first: 10) {{
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
                    group {{
                      id
                      code
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
        beneficiary_data = response_data['data']['groupBeneficiary']
        self.assertEqual(beneficiary_data['totalCount'], 3)

        enrolled_group_codes = list(
            e['node']['group']['code'] for e in beneficiary_data['edges']
        )
        self.assertTrue(self.group_0child.code in enrolled_group_codes)
        self.assertTrue(self.group_1child.code in enrolled_group_codes)
        self.assertTrue(self.group_2child.code in enrolled_group_codes)
        self.assertFalse(self.group_not_enrolled.code in enrolled_group_codes)

        # eligibility is status specific, so None is expected for all records without status filter
        eligible_none = list(
            e['node']['isEligible'] is None for e in beneficiary_data['edges']
        )
        self.assertTrue(all(eligible_none))


    def test_query_beneficiary_custom_filter(self):
        query_str = f"""
            query {{
              groupBeneficiary(
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
                    group {{
                      id
                      code
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

        beneficiary_data = response_data['data']['groupBeneficiary']
        self.assertEqual(beneficiary_data['totalCount'], 3)

        returned_group_codes = list(
            e['node']['group']['code'] for e in beneficiary_data['edges']
        )
        self.assertTrue(self.group_0child.code in returned_group_codes)
        self.assertTrue(self.group_1child.code in returned_group_codes)
        # group_2child also included because it contains individuals with < 2 children
        self.assertTrue(self.group_2child.code in returned_group_codes)

        query_str = query_str.replace('__lt__', '__gte__')

        response = self.query(query_str,
                              headers={"HTTP_AUTHORIZATION": f"Bearer {self.user_token}"})
        self.assertResponseNoErrors(response)
        response_data = json.loads(response.content)

        beneficiary_data = response_data['data']['groupBeneficiary']
        self.assertEqual(beneficiary_data['totalCount'], 1)

        beneficiary_node = beneficiary_data['edges'][0]['node']
        group_data = beneficiary_node['group']
        self.assertEqual(group_data['code'], self.group_2child.code)


    def test_query_beneficiary_status_filter(self):
        query_str = f"""
            query {{
              groupBeneficiary(
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
                    group {{
                      id
                      code
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

        beneficiary_data = response_data['data']['groupBeneficiary']
        self.assertEqual(beneficiary_data['totalCount'], 2)

        enrolled_group_codes = list(
            e['node']['group']['code'] for e in beneficiary_data['edges']
        )
        self.assertFalse(self.group_0child.code in enrolled_group_codes)
        self.assertTrue(self.group_1child.code in enrolled_group_codes)
        self.assertTrue(self.group_2child.code in enrolled_group_codes)
        self.assertFalse(self.group_not_enrolled.code in enrolled_group_codes)

        def find_beneficiary_by_code(code):
            for edge in beneficiary_data['edges']:
                if edge['node']['group']['code'] == code:
                    return edge['node']
            return None

        beneficiary_1child = find_beneficiary_by_code(self.group_1child.code)
        self.assertFalse(beneficiary_1child['isEligible'])

        beneficiary_2child = find_beneficiary_by_code(self.group_2child.code)
        self.assertTrue(beneficiary_2child['isEligible'])


    def test_query_beneficiary_eligible_filter(self):
        query_str = f"""
            query {{
              groupBeneficiary(
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
                    group {{
                      id
                      code
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

        beneficiary_data = response_data['data']['groupBeneficiary']
        self.assertEqual(beneficiary_data['totalCount'], 1)

        eligible_beneficiary = beneficiary_data['edges'][0]['node']
        self.assertTrue(eligible_beneficiary['isEligible'])
        self.assertEqual(self.group_2child.code, eligible_beneficiary['group']['code'])

        # flip search criteria and result should only return ineligible records
        query_str = query_str.replace('isEligible: true', 'isEligible: false')

        response = self.query(query_str,
                              headers={"HTTP_AUTHORIZATION": f"Bearer {self.user_token}"})
        self.assertResponseNoErrors(response)
        response_data = json.loads(response.content)

        beneficiary_data = response_data['data']['groupBeneficiary']
        self.assertEqual(beneficiary_data['totalCount'], 1)

        eligible_beneficiary = beneficiary_data['edges'][0]['node']
        self.assertFalse(eligible_beneficiary['isEligible'])
        self.assertEqual(self.group_1child.code, eligible_beneficiary['group']['code'])

    def test_query_group_beneficiary_village_or_child_of_filter(self):
        child_village = create_test_village({'code': 'BeneV1', 'name': 'Beneficiary Village 1'})
        parent_location = child_village.parent

        # Create a new group in the test village and enroll them
        _, village_group, _ = create_group_with_individual(self.user.username, group_override={
            "code": "VillageGroup",
            "location_id": child_village.id,
        })
        add_group_to_benefit_plan(self.service, village_group, self.benefit_plan)

        # Create a control group elsewhere
        another_village = create_test_village({'code': 'BeneV2', 'name': 'Beneficiary Village 2'})
        _, other_group, _ = create_group_with_individual(self.user.username, group_override={
            "code": "OtherGroup",
            "location_id": another_village.id,
        })
        add_group_to_benefit_plan(self.service, other_group, self.benefit_plan)

        # Run the query with villageOrChildOf = parent district ID
        query_str = f"""
        query {{
          groupBeneficiary(
            benefitPlan_Id: "{self.benefit_plan.uuid}",
            villageOrChildOf: {parent_location.id},
            isDeleted: false,
            first: 10
          ) {{
            totalCount
            edges {{
              node {{
                group {{
                  code
                }}
              }}
            }}
          }}
        }}
        """
        response = self.query(query_str, headers={"HTTP_AUTHORIZATION": f"Bearer {self.user_token}"})
        self.assertResponseNoErrors(response)
        data = json.loads(response.content)['data']['groupBeneficiary']

        self.assertEqual(data['totalCount'], 1)
        self.assertEqual(data['edges'][0]['node']['group']['code'], "VillageGroup")

    def test_project_beneficiary_enrollment(self):
        project = create_project(
            'test enrollment project',
            self.benefit_plan,
            self.user.username,
        )

        query_str = f'''
            mutation {{
              enrollGroupProject(
                input: {{
                  ids: ["{self.group_1child.id}", "{self.group_2child.id}"]
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
        internal_id = content['data']['enrollGroupProject']['internalId']
        self.assert_mutation_success(internal_id, self.user_token)
