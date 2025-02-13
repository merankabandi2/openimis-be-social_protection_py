import copy

from django.test import TestCase

from individual.models import Individual
from individual.tests.data import service_add_individual_payload

from social_protection.models import Beneficiary, BenefitPlan
from social_protection.services import BeneficiaryService
from social_protection.tests.data import (
    service_add_payload,
    service_beneficiary_add_payload,
    service_beneficiary_update_status_active_payload
)
from core.test_helpers import LogInHelper
from social_protection.tests.test_helpers import create_benefit_plan, create_individual


class BeneficiaryServiceTest(TestCase):
    user = None
    service = None
    query_all = None

    @classmethod
    def setUpClass(cls):
        super().setUpClass()

        cls.user = LogInHelper().get_or_create_user_api()
        cls.service = BeneficiaryService(cls.user)
        cls.query_all = Beneficiary.objects.filter(is_deleted=False)
        cls.benefit_plan = create_benefit_plan(cls.user.username, payload_override={
            'code': 'IMAX1',
            'type': "INDIVIDUAL",
            'max_beneficiaries': 1
        })

        cls.benefit_plan_no_max = create_benefit_plan(cls.user.username, payload_override={
            'code': 'INOMAX',
            'type': "INDIVIDUAL",
            'max_beneficiaries': None
        })

        cls.individual = create_individual(cls.user.username)
        cls.individual2 = create_individual(cls.user.username, payload_override={
            'first_name': "Second"
        })
        cls.individual3 = create_individual(cls.user.username, payload_override={
            'first_name': "Third"
        })

    def add_beneficiary_return_result(self, individual: Individual, benefit_plan: BenefitPlan = None, status="POTENTIAL"):
        benefit_plan = benefit_plan or self.benefit_plan
        payload = {
            **service_beneficiary_add_payload,
            "individual_id": individual.id,
            "benefit_plan_id": benefit_plan.id,
            "status": status
        }
        result = self.service.create(payload)
        return result

    def add_beneficiary_return_uuid(self, individual: Individual, benefit_plan: BenefitPlan = None, status="POTENTIAL"):
        result = self.add_beneficiary_return_result(individual, benefit_plan, status)
        self.assertTrue(result.get('success', False), result.get('detail', "No details provided"))
        return result.get('data', {}).get('uuid')

    def check_beneficiary_exists(self, uuid, with_status=None):
        query = self.query_all.filter(uuid=uuid)
        self.assertEqual(query.count(), 1)
        if with_status:
            self.assertEqual(query.first().status, with_status)
        
    def check_active_beneficiaries_count_eq(self, count, benefit_plan, msg=None):
        active_beneficiaries = self.query_all.filter(benefit_plan_id=benefit_plan.id, status="ACTIVE").distinct()
        self.assertEqual(active_beneficiaries.count(), count, msg)

    def test_add_beneficiary(self):
        uuid = self.add_beneficiary_return_uuid(self.individual, self.benefit_plan, status="POTENTIAL")
        self.check_beneficiary_exists(uuid, with_status="POTENTIAL")

        self.assertEqual(self.benefit_plan.max_beneficiaries, 1)

        uuid = self.add_beneficiary_return_uuid(self.individual2, self.benefit_plan, status="ACTIVE")
        self.check_beneficiary_exists(uuid, with_status="ACTIVE")
        self.check_active_beneficiaries_count_eq(1, self.benefit_plan, "One active beneficiary should have been added")

        result = self.add_beneficiary_return_result(self.individual3, self.benefit_plan, status="ACTIVE")
        self.assertFalse(result.get('success', True), "Benefit plan's 'max active beneficiaries' was not enforced")
        self.assertEqual(self.query_all.filter(individual__first_name=self.individual3.first_name).count(), 0)
        self.check_active_beneficiaries_count_eq(1, self.benefit_plan, "Second active beneficiary creation should have been blocked")
        
        self.assertEqual(self.benefit_plan_no_max.max_beneficiaries, None)

        for i, individual in enumerate([self.individual, self.individual2]):
            uuid = self.add_beneficiary_return_uuid(individual, self.benefit_plan_no_max, status="ACTIVE")
            self.check_beneficiary_exists(uuid, with_status="ACTIVE")
            self.check_active_beneficiaries_count_eq(i+1, self.benefit_plan_no_max, f"{i+1} beneficiaries should be added and active")

    def test_update_beneficiary(self):
        def create_and_update_to_active(individual, benefit_plan):
            uuid = self.add_beneficiary_return_uuid(individual, benefit_plan)
            update_payload = {
                **service_beneficiary_update_status_active_payload,
                'id': uuid,
                'individual_id': individual.id,
                'benefit_plan_id': benefit_plan.id
            }
            return self.service.update(update_payload), uuid
        
        self.assertEqual(self.benefit_plan.max_beneficiaries, 1)

        result, uuid = create_and_update_to_active(self.individual, self.benefit_plan)
        self.assertTrue(result.get('success', False), result.get('detail', "No details provided"))
        self.check_beneficiary_exists(uuid, with_status="ACTIVE")
        self.check_active_beneficiaries_count_eq(1, self.benefit_plan, "One active beneficiary should have been added")

        result, uuid = create_and_update_to_active(self.individual, self.benefit_plan)
        self.assertFalse(result.get('success', True), "Benefit plan's 'max active beneficiaries' was not enforced")
        self.check_beneficiary_exists(uuid, with_status="POTENTIAL")
        self.check_active_beneficiaries_count_eq(1, self.benefit_plan, "Second active beneficiary update should have been blocked")

        self.assertEqual(self.benefit_plan_no_max.max_beneficiaries, None)

        for i, individual in enumerate([self.individual, self.individual2]):
            result, uuid = create_and_update_to_active(individual, self.benefit_plan_no_max)
            self.assertTrue(result.get('success', False), result.get('detail', "No details provided"))
            self.check_beneficiary_exists(uuid, "ACTIVE")
            self.check_active_beneficiaries_count_eq(i+1, self.benefit_plan_no_max, f"{i+1} beneficiaries should be added and active")

    def test_delete_beneficiary(self):
        uuid = self.add_beneficiary_return_uuid(self.individual)
        delete_payload = {'id': uuid}
        result = self.service.delete(delete_payload)
        self.assertTrue(result.get('success', False), result.get('detail', "No details provided"))
        query = self.query_all.filter(uuid=uuid)
        self.assertEqual(query.count(), 0)
