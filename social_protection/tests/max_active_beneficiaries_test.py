import threading
import time
from unittest.mock import patch

from django.db import connection, connections
from django.test import TestCase, TransactionTestCase
from django.test.utils import CaptureQueriesContext

from core.services import BaseService
from core.test_helpers import LogInHelper
from individual.models import Group, Individual
from social_protection.models import Beneficiary, BenefitPlan, GroupBeneficiary
from social_protection.services import BeneficiaryService, GroupBeneficiaryService
from social_protection.tests.data import service_beneficiary_add_payload
from social_protection.tests.test_helpers import (
    create_benefit_plan,
    create_group,
    create_individual,
    generate_random_string,
)

INDIVIDUAL = {
    'service': BeneficiaryService,
    'model': Beneficiary,
    'plan_type': 'INDIVIDUAL',
    'subject_field': 'individual_id',
    'make_subject': lambda username: create_individual(
        username, payload_override={'first_name': generate_random_string()}),
}
GROUP = {
    'service': GroupBeneficiaryService,
    'model': GroupBeneficiary,
    'plan_type': 'GROUP',
    'subject_field': 'group_id',
    'make_subject': lambda username: create_group(username),
}


def _payload(config, plan, subject, status="ACTIVE"):
    return {
        **service_beneficiary_add_payload,
        config['subject_field']: subject.id,
        "benefit_plan_id": plan.id,
        "status": status,
    }


class MaxActiveBeneficiariesTestMixin:
    config = None

    @classmethod
    def setUpUser(cls):
        cls.user = LogInHelper().get_or_create_user_api()

    def new_plan(self, max_beneficiaries):
        return create_benefit_plan(self.user.username, payload_override={
            'code': generate_random_string(8),
            'type': self.config['plan_type'],
            'max_beneficiaries': max_beneficiaries,
        })

    def create(self, plan, subject, status="ACTIVE"):
        service = self.config['service'](self.user)
        return service.create(_payload(self.config, plan, subject, status))

    def active_count(self, plan):
        return self.config['model'].objects.filter(
            is_deleted=False, benefit_plan_id=plan.id, status="ACTIVE").count()


class MaxActiveBeneficiariesTest(MaxActiveBeneficiariesTestMixin):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.setUpUser()

    def test_refused_when_over_the_maximum(self):
        plan = self.new_plan(3)
        for _ in range(3):
            self.assertTrue(self.create(plan, self.config['make_subject'](self.user.username))['success'])
        BenefitPlan.objects.filter(id=plan.id).update(max_beneficiaries=2)
        self.assertEqual(self.active_count(plan), 3)

        result = self.create(plan, self.config['make_subject'](self.user.username))

        self.assertFalse(result['success'], "Creation over the maximum was accepted")
        self.assertEqual(self.active_count(plan), 3)

    def test_refused_when_at_the_maximum(self):
        plan = self.new_plan(1)
        self.assertTrue(self.create(plan, self.config['make_subject'](self.user.username))['success'])

        result = self.create(plan, self.config['make_subject'](self.user.username))

        self.assertFalse(result['success'])
        self.assertEqual(self.active_count(plan), 1)

    def test_update_to_active_refused_over_the_maximum(self):
        plan = self.new_plan(2)
        self.assertTrue(self.create(plan, self.config['make_subject'](self.user.username))['success'])
        self.assertTrue(self.create(plan, self.config['make_subject'](self.user.username))['success'])
        pending = self.create(plan, self.config['make_subject'](self.user.username), status="POTENTIAL")
        self.assertTrue(pending['success'])
        BenefitPlan.objects.filter(id=plan.id).update(max_beneficiaries=1)

        result = self.config['service'](self.user).update({
            'id': pending['data']['id'],
            'benefit_plan_id': plan.id,
            'status': 'ACTIVE',
        })

        self.assertFalse(result['success'], "Activation over the maximum was accepted")
        self.assertEqual(self.active_count(plan), 2)

    def test_already_active_record_can_be_updated_at_the_maximum(self):
        plan = self.new_plan(1)
        created = self.create(plan, self.config['make_subject'](self.user.username))
        self.assertTrue(created['success'])

        result = self.config['service'](self.user).update({
            'id': created['data']['id'],
            'benefit_plan_id': plan.id,
            'status': 'ACTIVE',
            'json_ext': {'note': 'same record'},
        })

        self.assertTrue(result['success'], result.get('detail'))

    def test_unlimited_plan_takes_no_lock(self):
        plan = self.new_plan(None)
        with CaptureQueriesContext(connection) as queries:
            for _ in range(2):
                self.assertTrue(self.create(plan, self.config['make_subject'](self.user.username))['success'])

        self.assertEqual(self.active_count(plan), 2)
        self.assertFalse([q for q in queries if 'FOR UPDATE' in q['sql']])

    def test_limited_plan_counts_under_a_row_lock(self):
        plan = self.new_plan(5)
        subject = self.config['make_subject'](self.user.username)
        with CaptureQueriesContext(connection) as queries:
            self.assertTrue(self.create(plan, subject)['success'])

        locks = [q['sql'] for q in queries if 'FOR UPDATE' in q['sql']]
        self.assertEqual(len(locks), 1)
        self.assertIn('social_protection_benefitplan', locks[0])


class IndividualMaxActiveBeneficiariesTest(MaxActiveBeneficiariesTest, TestCase):
    config = INDIVIDUAL


class GroupMaxActiveBeneficiariesTest(MaxActiveBeneficiariesTest, TestCase):
    config = GROUP


class MaxActiveBeneficiariesConcurrencyTestMixin(MaxActiveBeneficiariesTestMixin):
    """
    Runs without the test-runner transaction so that two connections can contend.
    The database is not flushed afterwards; the rows created here are deleted in tearDown.
    """

    def _fixture_teardown(self):
        pass

    def setUp(self):
        self.setUpUser()
        self.plans = []
        self.subjects = []

    def tearDown(self):
        for plan in self.plans:
            self.config['model'].objects.filter(benefit_plan_id=plan.id).delete()
            BenefitPlan.objects.filter(id=plan.id).delete()
        subject_model = Individual if self.config['plan_type'] == 'INDIVIDUAL' else Group
        subject_model.objects.filter(id__in=[s.id for s in self.subjects]).delete()

    def test_two_creations_at_the_last_place_accept_one(self):
        plan = self.new_plan(1)
        self.plans.append(plan)
        self.subjects = [self.config['make_subject'](self.user.username) for _ in range(2)]
        original_create = BaseService.create
        results = []

        def slow_create(service, obj_data):
            # Widens the window between the check and the write.
            time.sleep(0.5)
            return original_create(service, obj_data)

        def worker(subject):
            try:
                results.append(self.create(plan, subject))
            finally:
                connections.close_all()

        with patch.object(BaseService, 'create', slow_create):
            threads = [threading.Thread(target=worker, args=(s,)) for s in self.subjects]
            for t in threads:
                t.start()
            for t in threads:
                t.join()

        self.assertEqual(len(results), 2)
        self.assertEqual(sum(1 for r in results if r['success']), 1, results)
        self.assertEqual(self.active_count(plan), 1)


class IndividualMaxActiveBeneficiariesConcurrencyTest(MaxActiveBeneficiariesConcurrencyTestMixin, TransactionTestCase):
    config = INDIVIDUAL


class GroupMaxActiveBeneficiariesConcurrencyTest(MaxActiveBeneficiariesConcurrencyTestMixin, TransactionTestCase):
    config = GROUP
