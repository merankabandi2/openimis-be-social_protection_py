from unittest import mock
from django.utils.translation import gettext
from core.models import User
from core.models.openimis_graphql_test_case import BaseTestContext
from core.test_helpers import create_test_interactive_user
from social_protection import schema as sp_schema
from graphene import Schema
from social_protection.tests.test_helpers import (
    PatchedOpenIMISGraphQLTestCase,
    create_benefit_plan,
    create_individual,
    add_individual_to_benefit_plan,
    create_project,
)
from social_protection.models import (
    BeneficiaryProjectTimeEntry, Beneficiary, BeneficiaryProjectEnrollment,
    BeneficiaryStatus,
)
from social_protection.services import BeneficiaryService
from social_protection.apps import SocialProtectionConfig
from core.models import Role, RoleRight, UserRole
from location.test_helpers import create_test_village
import json


class BeneficiaryGQLTest(PatchedOpenIMISGraphQLTestCase):
    schema = Schema(query=sp_schema.Query)

    class AnonymousUserContext:
        user = mock.Mock(is_anonymous=True)

    @classmethod
    def _add_permissions_to_user(cls, user, permission_codes):
        """Add specific permissions to a user"""
        if hasattr(user, 'i_user') and user.i_user:
            role = Role.objects.create(
                name=f"TestRole_{user.username}",
                is_system=0,
                is_blocked=False,
                audit_user_id=-1
            )

            for perm_code in permission_codes:
                RoleRight.objects.create(
                    role=role,
                    right_id=int(perm_code),
                    audit_user_id=-1
                )

            UserRole.objects.create(
                user=user.i_user,
                role=role,
                audit_user_id=-1
            )

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.user = User.objects.filter(username='Admin', i_user__isnull=False).first()
        if not cls.user:
            cls.user = create_test_interactive_user(username='Admin')
        # some test data so as to created contract properly
        cls.user_token = BaseTestContext(user=cls.user).get_jwt()

        cls.test_officer = create_test_interactive_user(
            username="beneficiaryUserNoRight", roles=[1])
        cls.test_officer_token = BaseTestContext(user=cls.test_officer).get_jwt()

        cls.enroll_user = create_test_interactive_user(
            username="beneficiaryEnrollUser", roles=[1])
        cls._add_permissions_to_user(cls.enroll_user, SocialProtectionConfig.gql_project_beneficiary_enroll_perms)
        cls.enroll_user_token = BaseTestContext(user=cls.enroll_user).get_jwt()

        cls.time_entry_user = create_test_interactive_user(
            username="beneficiaryTimeEntryUser", roles=[1])
        cls._add_permissions_to_user(cls.time_entry_user, SocialProtectionConfig.gql_project_beneficiary_time_entry_perms)
        cls.time_entry_user_token = BaseTestContext(user=cls.time_entry_user).get_jwt()
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
        cls.individual = create_individual(cls.user.username, payload_override={
            'first_name': 'NoChild',
            'json_ext': {
                'number_of_children': 0
            }
        })
        cls.individual_not_enrolled = create_individual(cls.user.username, payload_override={
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
            }}""", headers={"HTTP_AUTHORIZATION": f"Bearer {self.user_token}"})
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

        # Create enrollment for the ACTIVE beneficiary
        beneficiary = self.individual.beneficiary_set.filter(benefit_plan=self.benefit_plan).first()
        if beneficiary.status != BeneficiaryStatus.ACTIVE:
            beneficiary.status = BeneficiaryStatus.ACTIVE
            beneficiary.save(username=self.user.username)
        enrollment = BeneficiaryProjectEnrollment(beneficiary=beneficiary, project=project)
        enrollment.save(user=self.user)

        # Query with enrolled_in_project filter
        query_str = f"""
            query {{
              beneficiary(
                enrolledInProject: "{project.id}",
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
                    projectEnrollments {{
                      project {{
                        id
                        name
                      }}
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
        self.assertEqual(returned_node['projectEnrollments'][0]['project']['name'], project.name)

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
            'test enrollment permission project',
            self.benefit_plan,
            self.user.username,
        )

        # Get the beneficiary ID (not individual ID)
        beneficiary = Beneficiary.objects.filter(
            individual=self.individual,
            benefit_plan=self.benefit_plan
        ).first()
        beneficiary_id = beneficiary.id

        query_str = f'''
            mutation {{
              enrollProject(
                input: {{
                  ids: ["{beneficiary_id}"]
                  projectId: "{str(project.id)}"
                }}
              ) {{
                clientMutationId
                internalId
              }}
            }}
        '''

        # Test for unauthenticated user
        response = self.query(query_str)
        self.assertResponseNoErrors(response)
        data = json.loads(response.content)['data']['enrollProject']
        # The mutation raises the translated messages of these keys.
        self.assert_mutation_error(
            data['internalId'], self.user_token, gettext('mutation.authentication_required'))

        # Test for user without permission (test_officer)
        response = self.query(
            query_str,
            headers={"HTTP_AUTHORIZATION": f"Bearer {self.test_officer_token}"}
        )
        self.assertResponseNoErrors(response)
        data = json.loads(response.content)['data']['enrollProject']
        self.assert_mutation_error(data['internalId'], self.test_officer_token, gettext('unauthorized'))

        # Test enrollment with authorized user
        response = self.query(
            query_str,
            headers={"HTTP_AUTHORIZATION": f"Bearer {self.enroll_user_token}"}
        )
        self.assertResponseNoErrors(response)
        data = json.loads(response.content)['data']['enrollProject']
        self.assert_mutation_success(data['internalId'], self.enroll_user_token)

        # Verify that expected project enrollment is persisted in the db
        enrollment = BeneficiaryProjectEnrollment.objects.filter(
            beneficiary=beneficiary,
            project=project,
            is_deleted=False
        ).first()
        self.assertIsNotNone(enrollment)

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

        # Enroll self.individual_2child in the exclusive project
        beneficiary_2child = self.individual_2child.beneficiary_set.filter(benefit_plan=self.benefit_plan).first()
        if beneficiary_2child.status != BeneficiaryStatus.ACTIVE:
            beneficiary_2child.status = BeneficiaryStatus.ACTIVE
            beneficiary_2child.save(username=self.user.username)
        enrollment_2child = BeneficiaryProjectEnrollment(beneficiary=beneficiary_2child, project=exclusive_project)
        enrollment_2child.save(user=self.user)

        # Enroll self.individual_1child in the multi project
        beneficiary_1child = self.individual_1child.beneficiary_set.filter(benefit_plan=self.benefit_plan).first()
        if beneficiary_1child.status != BeneficiaryStatus.ACTIVE:
            beneficiary_1child.status = BeneficiaryStatus.ACTIVE
            beneficiary_1child.save(username=self.user.username)
        enrollment_1child = BeneficiaryProjectEnrollment(beneficiary=beneficiary_1child, project=multi_project)
        enrollment_1child.save(user=self.user)

        # Query using multi-enrollment project filter — should exclude 2child,
        # include 1child & no-project
        query_str = f"""
            query {{
              beneficiary(
                benefitPlan_Id: "{self.benefit_plan.uuid}",
                eligibleForProject: "{multi_project.id}",
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
                eligibleForProject: "{exclusive_project.id}",
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

    def test_query_beneficiary_project_time_entries(self):
        project = create_project(
            'ProgressTrackingProject',
            self.benefit_plan,
            self.user.username,
            allows_multiple_enrollments=True,
        )
        beneficiary = self.individual.beneficiary_set.filter(benefit_plan=self.benefit_plan).first()
        if beneficiary.status != BeneficiaryStatus.ACTIVE:
            beneficiary.status = BeneficiaryStatus.ACTIVE
            beneficiary.save(username=self.user.username)
        enrollment = BeneficiaryProjectEnrollment(beneficiary=beneficiary, project=project)
        enrollment.save(user=self.user)

        BeneficiaryProjectTimeEntry(
            enrollment=enrollment, day_number=1, percent_complete=25
        ).save(username=self.user.username)
        BeneficiaryProjectTimeEntry(
            enrollment=enrollment, day_number=2, percent_complete=80
        ).save(username=self.user.username)
        BeneficiaryProjectTimeEntry(
            enrollment=enrollment, day_number=3, percent_complete=100
        ).save(username=self.user.username)

        query_str = f"""
        query {{
          beneficiary(
            enrolledInProject: "{project.id}",
            isDeleted: false,
            first: 10
          ) {{
            totalCount
            edges {{
              node {{
                individual {{
                  firstName
                }}
                projectEnrollments {{
                  project {{
                    id
                    name
                  }}
                  timeEntries {{
                    id
                    dayNumber
                    percentComplete
                  }}
                }}
              }}
            }}
          }}
        }}
        """

        response = self.query(
            query_str,
            headers={"HTTP_AUTHORIZATION": f"Bearer {self.user_token}"}
        )
        self.assertResponseNoErrors(response)
        response_data = json.loads(response.content)
        beneficiary_data = response_data['data']['beneficiary']

        self.assertEqual(beneficiary_data['totalCount'], 1)
        node = beneficiary_data['edges'][0]['node']

        # Verify basic beneficiary info
        self.assertEqual(node['individual']['firstName'], self.individual.first_name)
        self.assertEqual(node['projectEnrollments'][0]['project']['name'], project.name)

        # Verify time entries
        time_entries = node['projectEnrollments'][0]['timeEntries']
        self.assertEqual(len(time_entries), 3)

        # Check order and values
        sorted_entries = sorted(time_entries, key=lambda e: e['dayNumber'])
        self.assertEqual([e['dayNumber'] for e in sorted_entries], [1, 2, 3])
        self.assertEqual([e['percentComplete'] for e in sorted_entries], [25, 80, 100])

    def test_bulk_update_beneficiary_time_entries(self):
        project = create_project(
            'test time entry permission project',
            self.benefit_plan,
            self.user.username,
        )

        # Enroll beneficiary to project
        beneficiary = Beneficiary.objects.filter(
            individual=self.individual,
            benefit_plan=self.benefit_plan
        ).first()
        if beneficiary.status != BeneficiaryStatus.ACTIVE:
            beneficiary.status = BeneficiaryStatus.ACTIVE
            beneficiary.save(username=self.user.username)
        enrollment = BeneficiaryProjectEnrollment(
            beneficiary=beneficiary,
            project=project
        )
        enrollment.save(user=self.user)

        query_str = f'''
            mutation {{
              bulkUpdateBeneficiaryTimeEntries(
                input: {{
                  timeEntries: [{{
                    enrollmentId: "{str(enrollment.id)}"
                    dayNumber: 1
                    percentComplete: 50
                  }}]
                }}
              ) {{
                clientMutationId
                internalId
              }}
            }}
        '''

        # Test for unauthenticated user
        response = self.query(query_str)
        self.assertResponseNoErrors(response)
        data = json.loads(response.content)['data']['bulkUpdateBeneficiaryTimeEntries']
        self.assert_mutation_error(data['internalId'], self.user_token, 'authentication_required')

        # Test for user without permission (test_officer)
        response = self.query(
            query_str,
            headers={"HTTP_AUTHORIZATION": f"Bearer {self.test_officer_token}"}
        )
        self.assertResponseNoErrors(response)
        data = json.loads(response.content)['data']['bulkUpdateBeneficiaryTimeEntries']
        self.assert_mutation_error(data['internalId'], self.test_officer_token, 'unauthorized')

        # Test time entry update with authorized user
        response = self.query(
            query_str,
            headers={"HTTP_AUTHORIZATION": f"Bearer {self.time_entry_user_token}"}
        )
        self.assertResponseNoErrors(response)
        data = json.loads(response.content)['data']['bulkUpdateBeneficiaryTimeEntries']
        self.assert_mutation_success(data['internalId'], self.time_entry_user_token)

        # Verify that expected time entry is persisted in the db
        time_entry = BeneficiaryProjectTimeEntry.objects.filter(
            enrollment=enrollment,
            day_number=1
        ).first()
        self.assertIsNotNone(time_entry)
        self.assertEqual(time_entry.percent_complete, 50)
