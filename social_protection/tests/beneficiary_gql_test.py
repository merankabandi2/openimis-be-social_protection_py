from unittest import mock
import graphene
from core.models import User
from core.models.openimis_graphql_test_case import openIMISGraphQLTestCase, BaseTestContext
from core.test_helpers import create_test_interactive_user
from social_protection import schema as sp_schema
from graphene import Schema
from graphene.test import Client
from graphene_django.utils.testing import GraphQLTestCase
from django.conf import settings
from graphql_jwt.shortcuts import get_token
from social_protection.tests.test_helpers import create_benefit_plan,\
        create_individual, add_individual_to_benefit_plan
from social_protection.services import BeneficiaryService
import json

class BeneficiaryGQLTest(openIMISGraphQLTestCase):
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
