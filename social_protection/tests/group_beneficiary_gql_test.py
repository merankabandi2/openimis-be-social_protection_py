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
    create_group_with_individual,
    add_group_to_benefit_plan,
    create_individual,
    add_individual_to_group,
    create_project,
)
from social_protection.services import GroupBeneficiaryService
from social_protection.models import (
    GroupBeneficiaryProjectTimeEntry,
    GroupBeneficiary,
    GroupBeneficiaryProjectEnrollment,
)
from social_protection.apps import SocialProtectionConfig
from core.models import Role, RoleRight, UserRole
from location.test_helpers import create_test_village
import json


class GroupBeneficiaryGQLTest(PatchedOpenIMISGraphQLTestCase):
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
        super(GroupBeneficiaryGQLTest, cls).setUpClass()
        cls.user = User.objects.filter(username='Admin', i_user__isnull=False).first()
        if not cls.user:
            cls.user = create_test_interactive_user(username='Admin')
        cls.user_token = BaseTestContext(user=cls.user).get_jwt()

        cls.test_officer = create_test_interactive_user(
            username="grpBeneUserNoRight", roles=[1])
        cls.test_officer_token = BaseTestContext(user=cls.test_officer).get_jwt()

        cls.enroll_user = create_test_interactive_user(
            username="grpBeneEnrollUser", roles=[1])
        cls._add_permissions_to_user(cls.enroll_user, SocialProtectionConfig.gql_project_beneficiary_enroll_perms)
        cls.enroll_user_token = BaseTestContext(user=cls.enroll_user).get_jwt()

        cls.time_entry_user = create_test_interactive_user(
            username="grpBeneTimeEntryUser", roles=[1])
        cls._add_permissions_to_user(cls.time_entry_user, SocialProtectionConfig.gql_project_beneficiary_time_entry_perms)
        cls.time_entry_user_token = BaseTestContext(user=cls.time_entry_user).get_jwt()
        cls.benefit_plan = create_benefit_plan(cls.user.username, payload_override={
            'code': 'GGQLTest',
            'type': 'GROUP'
        })
        cls.individual_2child, cls.group_2child, gi = create_group_with_individual(
            cls.user.username,
            individual_override={
                'first_name': 'Alice',
                'last_name': 'Zulu',
                'dob': '2000-01-01',
                'json_ext': {
                    'number_of_children': 2
                }
            }
        )
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
        add_individual_to_group(cls.user.username, child1, cls.group_2child, is_head=False)
        add_individual_to_group(cls.user.username, child2, cls.group_2child, is_head=False)

        cls.individual_1child, cls.group_1child, __ = create_group_with_individual(
            cls.user.username,
            individual_override={
                'first_name': 'Bob',
                'last_name': 'Yankee',
                'dob': '1990-01-01',
                'json_ext': {
                    'number_of_children': 1
                }
            }
        )
        cls.individual, cls.group_0child, __ = create_group_with_individual(
            cls.user.username,
            individual_override={
                'first_name': 'Charlie',
                'last_name': 'Xray',
                'dob': '1980-01-01',
                'json_ext': {
                    'number_of_children': 0
                }
            }
        )
        cls.individual_not_enrolled, cls.group_not_enrolled, __ = create_group_with_individual(
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
            """, headers={"HTTP_AUTHORIZATION": f"Bearer {self.user_token}"})
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

        gb_1child = self.group_1child.groupbeneficiary_set.filter(benefit_plan=self.benefit_plan).first()
        gb_2child = self.group_2child.groupbeneficiary_set.filter(benefit_plan=self.benefit_plan).first()

        query_str = f'''
            mutation {{
              enrollGroupProject(
                input: {{
                  ids: ["{gb_1child.id}", "{gb_2child.id}"]
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

    def test_query_group_beneficiary_search(self):
        # search matches on group code
        _, search_group, _ = create_group_with_individual(self.user.username, group_override={
            "code": "SearchMatchGroup",
        })
        add_group_to_benefit_plan(self.service, search_group, self.benefit_plan)

        # search matches on head last name
        _, head_group, _ = create_group_with_individual(self.user.username, individual_override={
            "last_name": "Named Match",
        })
        add_group_to_benefit_plan(self.service, head_group, self.benefit_plan)

        # search matches on location name
        child_village = create_test_village({'code': 'BeneV1', 'name': 'Village Match'})
        _, village_group, _ = create_group_with_individual(self.user.username, group_override={
            "code": "VillageGroup",
            "location_id": child_village.id,
        })
        add_group_to_benefit_plan(self.service, village_group, self.benefit_plan)

        # search matches on json ext field value
        _, ext_group, _ = create_group_with_individual(self.user.username, group_override={
            "code": "JsonExtGroup",
            'json_ext': {
                'abc': 'json mAtch here',
            }
        })
        add_group_to_benefit_plan(self.service, ext_group, self.benefit_plan)

        response = self.query(
            f"""
            query {{
              groupBeneficiary(
                benefitPlan_Id: "{self.benefit_plan.uuid}",
                search: "match",
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
            """,
            headers={"HTTP_AUTHORIZATION": f"Bearer {self.user_token}"}
        )

        self.assertResponseNoErrors(response)
        data = json.loads(response.content)['data']['groupBeneficiary']

        group_codes = list(
            e['node']['group']['code'] for e in data['edges']
        )
        self.assertTrue(search_group.code in group_codes)
        self.assertTrue(head_group.code in group_codes)
        self.assertTrue(village_group.code in group_codes)
        self.assertTrue(ext_group.code in group_codes)
        self.assertEqual(data['totalCount'], 4)

    def test_query_group_beneficiary_filter_location(self):
        village = create_test_village({'code': 'LocV1', 'name': 'FfBLV'})
        district_name_partial = village.parent.parent.name.lower()[-5:]
        _, location_group, _ = create_group_with_individual(self.user.username, group_override={
            "code": "LocationGroup",
            "location_id": village.id,
        })
        add_group_to_benefit_plan(self.service, location_group, self.benefit_plan)

        another_village = create_test_village({'code': 'LocV2', 'name': 'XXZV'})
        _, another_group, _ = create_group_with_individual(self.user.username, group_override={
            "code": "AnotherGroup",
            "location_id": another_village.id,
        })
        add_group_to_benefit_plan(self.service, another_group, self.benefit_plan)

        response = self.query(
            f"""
            query {{
              groupBeneficiary(
                benefitPlan_Id: "{self.benefit_plan.uuid}",
                location: "1:{district_name_partial}",
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
            """,
            headers={"HTTP_AUTHORIZATION": f"Bearer {self.user_token}"}
        )

        self.assertResponseNoErrors(response)
        data = json.loads(response.content)['data']['groupBeneficiary']

        group_codes = list(
            e['node']['group']['code'] for e in data['edges']
        )
        self.assertTrue(location_group.code in group_codes)
        self.assertTrue(another_group.code not in group_codes)

        # update the query to look for "district" which would return both groups
        response = self.query(
            f"""
            query {{
              groupBeneficiary(
                benefitPlan_Id: "{self.benefit_plan.uuid}",
                location: "1:disTrict",
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
            """,
            headers={"HTTP_AUTHORIZATION": f"Bearer {self.user_token}"}
        )

        self.assertResponseNoErrors(response)
        data = json.loads(response.content)['data']['groupBeneficiary']

        group_codes = list(
            e['node']['group']['code'] for e in data['edges']
        )
        self.assertTrue(location_group.code in group_codes)
        self.assertTrue(another_group.code in group_codes)

    def test_query_group_beneficiary_allows_multiple_enrollments_filter(self):
        # Create two projects, one that allows multiple enrollments, one that does not
        multi_project = create_project(
            'MultiEnrollmentGroupProject',
            self.benefit_plan,
            self.user.username,
            allows_multiple_enrollments=True,
        )
        exclusive_project = create_project(
            'ExclusiveGroupProject',
            self.benefit_plan,
            self.user.username,
            allows_multiple_enrollments=False,
        )

        # Enroll group_2child into exclusive project
        gb_2child = self.group_2child.groupbeneficiary_set.filter(benefit_plan=self.benefit_plan).first()
        enrollment_2child = GroupBeneficiaryProjectEnrollment(group_beneficiary=gb_2child, project=exclusive_project)
        enrollment_2child.save(user=self.user)

        # Enroll group_1child into multi-enrollment project
        gb_1child = self.group_1child.groupbeneficiary_set.filter(benefit_plan=self.benefit_plan).first()
        enrollment_1child = GroupBeneficiaryProjectEnrollment(group_beneficiary=gb_1child, project=multi_project)
        enrollment_1child.save(user=self.user)

        # Query using multi-enrollment project: should exclude group_2child, include group_1child and group_0child
        query_str = f"""
            query {{
              groupBeneficiary(
                benefitPlan_Id: "{self.benefit_plan.uuid}",
                eligibleForProject: "{multi_project.id}",
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
        returned_codes = [e['node']['group']['code'] for e in data['edges']]

        self.assertIn(self.group_1child.code, returned_codes)
        self.assertIn(self.group_0child.code, returned_codes)
        self.assertNotIn(self.group_2child.code, returned_codes)

        # Query using exclusive project: should return only itself or unassigned
        query_str = f"""
            query {{
              groupBeneficiary(
                benefitPlan_Id: "{self.benefit_plan.uuid}",
                eligibleForProject: "{exclusive_project.id}",
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
        returned_codes = [e['node']['group']['code'] for e in data['edges']]

        self.assertIn(self.group_2child.code, returned_codes)
        self.assertIn(self.group_0child.code, returned_codes)
        self.assertNotIn(self.group_1child.code, returned_codes)

    def test_query_group_beneficiary_project_time_entries(self):
        project = create_project(
            'GroupProgressTrackingProject',
            self.benefit_plan,
            self.user.username,
            allows_multiple_enrollments=True,
        )
        # Enroll the ACTIVE group beneficiary in this project
        group_beneficiary = self.group_0child.groupbeneficiary_set.filter(
            benefit_plan=self.benefit_plan
        ).first()
        enrollment = GroupBeneficiaryProjectEnrollment(
            group_beneficiary=group_beneficiary, project=project
        )
        enrollment.save(user=self.user)

        # Create time entries
        GroupBeneficiaryProjectTimeEntry(
            enrollment=enrollment, day_number=1, percent_complete=30
        ).save(username=self.user.username)
        GroupBeneficiaryProjectTimeEntry(
            enrollment=enrollment, day_number=2, percent_complete=60
        ).save(username=self.user.username)
        GroupBeneficiaryProjectTimeEntry(
            enrollment=enrollment, day_number=3, percent_complete=90
        ).save(username=self.user.username)

        # Query with nested projectEnrollments and timeEntries
        query_str = f"""
        query {{
          groupBeneficiary(
            enrolledInProject: "{project.id}",
            isDeleted: false,
            first: 10
          ) {{
            totalCount
            edges {{
              node {{
                group {{
                  code
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
        beneficiary_data = response_data['data']['groupBeneficiary']

        # Verify one group beneficiary
        self.assertEqual(beneficiary_data['totalCount'], 1)
        node = beneficiary_data['edges'][0]['node']

        # Verify group + project through enrollment
        self.assertEqual(node['group']['code'], self.group_0child.code)
        self.assertEqual(node['projectEnrollments'][0]['project']['name'], project.name)

        # Verify time entries
        time_entries = node['projectEnrollments'][0]['timeEntries']
        self.assertEqual(len(time_entries), 3)

        # Check order and values
        sorted_entries = sorted(time_entries, key=lambda e: e['dayNumber'])
        self.assertEqual([e['dayNumber'] for e in sorted_entries], [1, 2, 3])
        self.assertEqual([e['percentComplete'] for e in sorted_entries], [30, 60, 90])

    def test_project_group_beneficiary_enrollment(self):
        project = create_project(
            'test group enrollment permission project',
            self.benefit_plan,
            self.user.username,
        )

        # Get the group beneficiary ID (not group ID)
        group_beneficiary = GroupBeneficiary.objects.filter(
            group=self.group_0child,
            benefit_plan=self.benefit_plan
        ).first()
        group_beneficiary_id = group_beneficiary.id

        query_str = f'''
            mutation {{
              enrollGroupProject(
                input: {{
                  ids: ["{group_beneficiary_id}"]
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
        data = json.loads(response.content)['data']['enrollGroupProject']
        # The mutation raises the translated messages of these keys.
        self.assert_mutation_error(
            data['internalId'], self.user_token, gettext('mutation.authentication_required'))

        # Test for user without permission (test_officer)
        response = self.query(
            query_str,
            headers={"HTTP_AUTHORIZATION": f"Bearer {self.test_officer_token}"}
        )
        self.assertResponseNoErrors(response)
        data = json.loads(response.content)['data']['enrollGroupProject']
        self.assert_mutation_error(data['internalId'], self.test_officer_token, gettext('unauthorized'))

        # Test for user with enrollment permission
        response = self.query(
            query_str,
            headers={"HTTP_AUTHORIZATION": f"Bearer {self.enroll_user_token}"}
        )
        self.assertResponseNoErrors(response)
        data = json.loads(response.content)['data']['enrollGroupProject']
        self.assert_mutation_success(data['internalId'], self.enroll_user_token)

        # Verify that expected project enrollment is persisted in the db
        enrollment = GroupBeneficiaryProjectEnrollment.objects.filter(
            group_beneficiary=group_beneficiary,
            project=project,
            is_deleted=False
        ).first()
        self.assertIsNotNone(enrollment)

    def test_bulk_update_group_beneficiary_time_entries(self):
        project = create_project(
            'test group time entry permission project',
            self.benefit_plan,
            self.user.username,
        )

        # Enroll group beneficiary to project
        group_beneficiary = GroupBeneficiary.objects.filter(
            group=self.group_0child,
            benefit_plan=self.benefit_plan
        ).first()
        enrollment = GroupBeneficiaryProjectEnrollment(
            group_beneficiary=group_beneficiary,
            project=project
        )
        enrollment.save(user=self.user)

        query_str = f'''
            mutation {{
              bulkUpdateGroupBeneficiaryTimeEntries(
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
        data = json.loads(response.content)['data']['bulkUpdateGroupBeneficiaryTimeEntries']
        self.assert_mutation_error(data['internalId'], self.user_token, 'authentication_required')

        # Test for user without permission (test_officer)
        response = self.query(
            query_str,
            headers={"HTTP_AUTHORIZATION": f"Bearer {self.test_officer_token}"}
        )
        self.assertResponseNoErrors(response)
        data = json.loads(response.content)['data']['bulkUpdateGroupBeneficiaryTimeEntries']
        self.assert_mutation_error(data['internalId'], self.test_officer_token, 'unauthorized')

        # Test for user with time entry permission
        response = self.query(
            query_str,
            headers={"HTTP_AUTHORIZATION": f"Bearer {self.time_entry_user_token}"}
        )
        self.assertResponseNoErrors(response)
        data = json.loads(response.content)['data']['bulkUpdateGroupBeneficiaryTimeEntries']
        self.assert_mutation_success(data['internalId'], self.time_entry_user_token)

        # Verify that expected time entry is persisted in the db
        time_entry = GroupBeneficiaryProjectTimeEntry.objects.filter(
            enrollment=enrollment,
            day_number=1
        ).first()
        self.assertIsNotNone(time_entry)
        self.assertEqual(time_entry.percent_complete, 50)

    def test_query_group_beneficiary_order_by_head_first_name(self):
        """Test ordering group beneficiaries by head's first name.

        Uses existing groups from setUpClass:
        - group_2child: head first_name='Alice'
        - group_1child: head first_name='Bob'
        - group_0child: head first_name='Charlie'
        """
        query_str = f"""
            query {{
              groupBeneficiary(
                benefitPlan_Id: "{self.benefit_plan.uuid}",
                orderBy: ["head_first_name"],
                isDeleted: false,
                first: 10
              ) {{
                edges {{
                  node {{
                    group {{
                      code
                      head {{
                        firstName
                      }}
                    }}
                  }}
                }}
              }}
            }}
        """
        response = self.query(query_str, headers={"HTTP_AUTHORIZATION": f"Bearer {self.user_token}"})
        self.assertResponseNoErrors(response)
        data = json.loads(response.content)['data']['groupBeneficiary']

        first_names = [e['node']['group']['head']['firstName'] for e in data['edges']]
        self.assertEqual(first_names, ['Alice', 'Bob', 'Charlie'])

        # Descending order
        query_str = query_str.replace('orderBy: ["head_first_name"]', 'orderBy: ["-head_first_name"]')
        response = self.query(query_str, headers={"HTTP_AUTHORIZATION": f"Bearer {self.user_token}"})
        self.assertResponseNoErrors(response)
        data = json.loads(response.content)['data']['groupBeneficiary']

        first_names = [e['node']['group']['head']['firstName'] for e in data['edges']]
        self.assertEqual(first_names, ['Charlie', 'Bob', 'Alice'])

    def test_query_group_beneficiary_order_by_head_last_name(self):
        """Test ordering group beneficiaries by head's last name.

        Uses existing groups from setUpClass:
        - group_0child: head last_name='Xray'
        - group_1child: head last_name='Yankee'
        - group_2child: head last_name='Zulu'
        """
        query_str = f"""
            query {{
              groupBeneficiary(
                benefitPlan_Id: "{self.benefit_plan.uuid}",
                orderBy: ["head_last_name"],
                isDeleted: false,
                first: 10
              ) {{
                edges {{
                  node {{
                    group {{
                      code
                      head {{
                        lastName
                      }}
                    }}
                  }}
                }}
              }}
            }}
        """
        response = self.query(query_str, headers={"HTTP_AUTHORIZATION": f"Bearer {self.user_token}"})
        self.assertResponseNoErrors(response)
        data = json.loads(response.content)['data']['groupBeneficiary']

        last_names = [e['node']['group']['head']['lastName'] for e in data['edges']]
        self.assertEqual(last_names, ['Xray', 'Yankee', 'Zulu'])

    def test_query_group_beneficiary_order_by_head_dob(self):
        """Test ordering group beneficiaries by head's date of birth.

        Uses existing groups from setUpClass:
        - group_0child: head dob='1980-01-01'
        - group_1child: head dob='1990-01-01'
        - group_2child: head dob='2000-01-01'
        """
        query_str = f"""
            query {{
              groupBeneficiary(
                benefitPlan_Id: "{self.benefit_plan.uuid}",
                orderBy: ["head_dob"],
                isDeleted: false,
                first: 10
              ) {{
                edges {{
                  node {{
                    group {{
                      code
                      head {{
                        dob
                      }}
                    }}
                  }}
                }}
              }}
            }}
        """
        response = self.query(query_str, headers={"HTTP_AUTHORIZATION": f"Bearer {self.user_token}"})
        self.assertResponseNoErrors(response)
        data = json.loads(response.content)['data']['groupBeneficiary']

        dobs = [e['node']['group']['head']['dob'] for e in data['edges']]
        self.assertEqual(dobs, ['1980-01-01', '1990-01-01', '2000-01-01'])

        # Descending order (youngest first)
        query_str = query_str.replace('orderBy: ["head_dob"]', 'orderBy: ["-head_dob"]')
        response = self.query(query_str, headers={"HTTP_AUTHORIZATION": f"Bearer {self.user_token}"})
        self.assertResponseNoErrors(response)
        data = json.loads(response.content)['data']['groupBeneficiary']

        dobs = [e['node']['group']['head']['dob'] for e in data['edges']]
        self.assertEqual(dobs, ['2000-01-01', '1990-01-01', '1980-01-01'])
