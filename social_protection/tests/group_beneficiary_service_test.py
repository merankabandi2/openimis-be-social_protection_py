import copy

from django.test import TestCase

from individual.models import Group

from social_protection.models import BenefitPlan, GroupBeneficiary
from social_protection.services import GroupBeneficiaryService
from social_protection.tests.data import (
    service_beneficiary_add_payload, service_beneficiary_update_status_active_payload,
)
from core.test_helpers import LogInHelper
from social_protection.tests.test_helpers import (
    create_benefit_plan, create_group
)
from datetime import datetime


class GroupBeneficiaryServiceTest(TestCase):
    user = None
    service = None
    query_all = None

    @classmethod
    def setUpClass(cls):
        super().setUpClass()

        cls.user = LogInHelper().get_or_create_user_api()
        cls.service = GroupBeneficiaryService(cls.user)
        cls.query_all = GroupBeneficiary.objects.filter(is_deleted=False)
        cls.benefit_plan = create_benefit_plan(cls.user.username, payload_override={
            'code': 'GMAX1',
            'type': "GROUP",
            'max_beneficiaries': 1
        })

        cls.benefit_plan_no_max = create_benefit_plan(cls.user.username, payload_override={
            'code': 'GNOMAX',
            'type': "GROUP",
            'max_beneficiaries': None
        })

        cls.group = create_group(cls.user.username)
        cls.group2 = create_group(cls.user.username)
        cls.group3 = create_group(cls.user.username)
    
    def add_beneficiary_return_result(self, group: Group, benefit_plan: BenefitPlan = None, status="POTENTIAL"):
        benefit_plan = benefit_plan or self.benefit_plan
        payload = {
            **service_beneficiary_add_payload,
            "group_id": group.id,
            "benefit_plan_id": benefit_plan.id,
            "status": status
        }
        result = self.service.create(payload)
        return result
        
    def add_beneficiary_return_uuid(self, group: Group, benefit_plan: BenefitPlan = None, status="POTENTIAL"):
        result = self.add_beneficiary_return_result(group, benefit_plan, status)
        self.assertTrue(result.get('success', False), result.get('detaul', "No details provided"))
        return result.get('data', {}).get('uuid', None)

    def check_beneficiary_exists(self, uuid, with_status):
        query = self.query_all.filter(uuid=uuid)
        self.assertEqual(query.count(), 1)
        if with_status:
            self.assertEqual(query.first().status, with_status)
        
    def check_active_beneficiaries_count_eq(self, count, benefit_plan, msg=None):
        active_beneficiaries = self.query_all.filter(benefit_plan_id=benefit_plan.id, status="ACTIVE").distinct()
        self.assertEqual(active_beneficiaries.count(), count, msg)

    def test_add_group_beneficiary(self):
        uuid = self.add_beneficiary_return_uuid(self.group, self.benefit_plan, status="POTENTIAL")
        self.check_beneficiary_exists(uuid, with_status="POTENTIAL")

        self.assertEqual(self.benefit_plan.max_beneficiaries, 1)

        uuid = self.add_beneficiary_return_uuid(self.group2, self.benefit_plan, status="ACTIVE")
        self.check_beneficiary_exists(uuid, with_status="ACTIVE")
        self.check_active_beneficiaries_count_eq(1, self.benefit_plan, "One active beneficiary should have been added")

        result = self.add_beneficiary_return_result(self.group3, self.benefit_plan, status="ACTIVE")
        self.assertFalse(result.get('success', True), "Benefit plan's 'max active beneficiaries' was not enforced")
        self.assertEqual(self.query_all.filter(group__code=self.group3.code).count(), 0)
        self.check_active_beneficiaries_count_eq(1, self.benefit_plan, "Second active beneficiary addition should have been blocked")
        
        self.assertEqual(self.benefit_plan_no_max.max_beneficiaries, None)

        for i, group in enumerate([self.group, self.group2]):
            uuid = self.add_beneficiary_return_uuid(group, self.benefit_plan_no_max, status="ACTIVE")
            self.check_beneficiary_exists(uuid, with_status="ACTIVE")
            self.check_active_beneficiaries_count_eq(i+1, self.benefit_plan_no_max, f"{i+1} beneficiaries should be added and active")

    def test_update_group_beneficiary(self):
        def create_and_update_to_active(group, benefit_plan):
            uuid = self.add_beneficiary_return_uuid(group, benefit_plan, status="POTENTIAL")
            update_payload = {
                **service_beneficiary_update_status_active_payload,
                'id': uuid,
                'group_id': group.id,
                'benefit_plan_id': benefit_plan.id
            }
            return self.service.update(update_payload), uuid
        
        self.assertEqual(self.benefit_plan.max_beneficiaries, 1)

        result, uuid = create_and_update_to_active(self.group, self.benefit_plan)
        self.assertTrue(result.get('success', False), result.get('detail', "No details provided"))
        self.check_beneficiary_exists(uuid, with_status="ACTIVE")
        self.check_active_beneficiaries_count_eq(1, self.benefit_plan, "One active beneficiary should have been added")

        update_payload = {
            **service_beneficiary_update_status_active_payload,
            'id': uuid,
            'group_id': self.group.id,
            'benefit_plan_id': self.benefit_plan.id,
            'json_ext': {
                'email': 'foo.bar@example.com',
                'able_bodied': True,
                'number_of_children': 2,
            }
        }
        result = self.service.update(update_payload)
        self.assertTrue(result.get('success', False), result.get('detail', "No details provided"))
        self.check_beneficiary_exists(uuid, with_status="ACTIVE")
        self.check_active_beneficiaries_count_eq(1, self.benefit_plan, "One active beneficiary should have been added")

        result, uuid = create_and_update_to_active(self.group2, self.benefit_plan)
        self.assertFalse(result.get('success', True), "Benefit plan's 'max active beneficiaries' was not enforced")
        self.check_beneficiary_exists(uuid, with_status="POTENTIAL")
        self.check_active_beneficiaries_count_eq(1, self.benefit_plan, "Second active beneficiary update should have been blocked")

        self.assertEqual(self.benefit_plan_no_max.max_beneficiaries, None)

        for i, group in enumerate([self.group, self.group2]):
            result, uuid = create_and_update_to_active(group, self.benefit_plan_no_max)
            self.assertTrue(result.get('success', False), result.get('detail', "No details provided"))
            self.check_beneficiary_exists(uuid, with_status="ACTIVE")
            self.check_active_beneficiaries_count_eq(i+1, self.benefit_plan_no_max, f"{i+1} beneficiaries should be added and active")

    def test_delete_group_beneficiary(self):
        uuid = self.add_beneficiary_return_uuid(self.group)
        delete_payload = {'id': uuid}
        result = self.service.delete(delete_payload)
        self.assertTrue(result.get('success', False), result.get('detail', "No details provided"))
        query = self.query_all.filter(uuid=uuid)
        self.assertEqual(query.count(), 0)
