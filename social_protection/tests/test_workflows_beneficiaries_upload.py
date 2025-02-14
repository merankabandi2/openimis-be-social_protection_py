from django.db import connection
from django.test import TestCase
from core.test_helpers import create_test_interactive_user
from individual.models import (
    Individual,
    IndividualDataSource,
    IndividualDataSourceUpload,
)
from social_protection.models import Beneficiary, BenefitPlan, BenefitPlanDataUploadRecords
from social_protection.tests.test_helpers import create_benefit_plan
from social_protection.workflows.base_beneficiary_upload import process_import_beneficiaries_workflow
from location.test_helpers import create_test_village, assign_user_districts
from unittest.mock import patch
from unittest import skipIf


@skipIf(
    connection.vendor != "postgresql",
    "Skipping tests due to implementation usage of validate_json_schema, which is a postgres specific extension."
)
class ProcessImportBeneficiariesWorkflowTest(TestCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        # Patch validate_dataframe_headers as it is already tested separately
        cls.validate_headers_patcher = patch(
            "social_protection.workflows.utils.BasePythonWorkflowExecutor.validate_dataframe_headers",
            lambda self: None
        )
        cls.validate_headers_patcher.start()

        cls.schema_patcher = patch("individual.apps.IndividualConfig.individual_schema", "{}")
        cls.schema_patcher.start()

    @classmethod
    def tearDownClass(cls):
        cls.validate_headers_patcher.stop()
        cls.schema_patcher.stop()
        super().tearDownClass()

    def setUp(self):
        self.user = create_test_interactive_user(username="admin")
        self.user_uuid = self.user.id
        
        self.plan_schema = {"properties": {}}

        # create BenefitPlan with the required fields and save with username
        self.benefit_plan = create_benefit_plan(self.user.username, {
                "name": "Test Benefit Plan",
                "description": "A test benefit plan",
                "code": "TESTPlan",
                "max_beneficiaries": 1000,
                "beneficiary_data_schema": {}
            })

        self.upload = IndividualDataSourceUpload(
            source_name='csv',
            source_type='upload',
            status="PENDING",
        )
        self.upload.save(user=self.user)
        self.upload_uuid = self.upload.id

        upload_record = BenefitPlanDataUploadRecords(
            data_upload=self.upload,
            workflow='Python Beneficiaries Upload',
            benefit_plan=self.benefit_plan,
            json_ext={}
        )
        upload_record.save(user=self.user.user)

        self.village = create_test_village({
            'name': 'McLean',
            'code': 'VwA',
        })
        self.valid_data_source = IndividualDataSource(
            upload_id=self.upload_uuid,
            json_ext={
                "first_name": "John",
                "last_name": "Doe",
                "dob": "1980-01-01",
                "location_name": self.village.name,
                "location_code": self.village.code,
            }
        )
        self.valid_data_source.save(user=self.user)

        self.invalid_data_source = IndividualDataSource(
            upload_id=self.upload_uuid,
            json_ext={
                "first_name": "Jane Workflow",
            }
        )
        self.invalid_data_source.save(user=self.user)

    @patch('individual.apps.IndividualConfig.enable_maker_checker_for_individual_upload', False)
    @patch('social_protection.apps.SocialProtectionConfig.enable_maker_checker_for_beneficiary_upload', False)
    def test_process_import_beneficiaries_workflow_successful_execution(self):
        process_import_beneficiaries_workflow(self.user_uuid, self.benefit_plan.uuid, self.upload_uuid)

        upload = IndividualDataSourceUpload.objects.get(id=self.upload_uuid)

        # Check that the status is 'FAIL' due to missing fields in one entry
        # print(vars(upload))
        self.assertEqual(upload.status, "FAIL")
        self.assertIsNotNone(upload.error)
        errors = upload.error['errors']
        self.assertIn("Invalid entries", errors['error'])

        # Check that the correct failing entries are logged in the error field
        for key in [
            "failing_entries_last_name", "failing_entries_dob"
        ]:
            self.assertIn(key, errors)
            self.assertIn(str(self.invalid_data_source.id), errors[key])
            self.assertNotIn(str(self.valid_data_source.id), errors[key])

        # individual_id should not be assigned for any data sources
        data_entries = IndividualDataSource.objects.filter(upload_id=self.upload_uuid)
        for entry in data_entries:
            self.assertIsNone(entry.individual_id)

    @patch('individual.apps.IndividualConfig.enable_maker_checker_for_individual_upload', False)
    @patch('individual.apps.IndividualConfig.enable_maker_checker_for_group_upload', False)
    @patch('social_protection.apps.SocialProtectionConfig.enable_maker_checker_for_beneficiary_upload', False)
    def test_process_import_beneficiaries_workflow_with_all_valid_entries(self):
        # Update invalid entry in IndividualDataSource to valid data
        self.invalid_data_source.json_ext={
            "first_name": "Jane Workflow",
            "last_name": "Doe",
            "dob": "1982-01-01",
            "location_name": None,
            "location_code": None,
        }
        self.invalid_data_source.save(user=self.user)

        process_import_beneficiaries_workflow(self.user_uuid, self.benefit_plan.uuid, self.upload_uuid)

        upload = IndividualDataSourceUpload.objects.get(id=self.upload_uuid)

        self.assertEqual(upload.status, "SUCCESS", upload.error)
        self.assertEqual(upload.error, {})

        # Verify that individual IDs have been assigned to data entries in IndividualDataSource
        data_entries = IndividualDataSource.objects.filter(upload_id=self.upload_uuid)
        for entry in data_entries:
            self.assertIsNotNone(entry.individual_id)

        # Check created individuals have the expected field values
        valid_ds = data_entries.get(id=self.valid_data_source.id)
        individual1 = Individual.objects.get(id=valid_ds.individual_id)
        json_ext1 = self.valid_data_source.json_ext
        self.assertEqual(individual1.first_name, json_ext1['first_name'])
        self.assertEqual(individual1.last_name, json_ext1['last_name'])
        self.assertEqual(individual1.dob.strftime('%Y-%m-%d'), json_ext1['dob'])
        self.assertEqual(individual1.location.name, json_ext1['location_name'])

        invalid_ds = data_entries.get(id=self.invalid_data_source.id)
        individual2 = Individual.objects.get(id=invalid_ds.individual_id)
        json_ext2 = self.invalid_data_source.json_ext
        self.assertEqual(individual2.first_name, json_ext2['first_name'])
        self.assertEqual(individual2.last_name, json_ext2['last_name'])
        self.assertEqual(individual2.dob.strftime('%Y-%m-%d'), json_ext2['dob'])
        self.assertIsNone(individual2.location)

        # Verify that the new individuals are added to the benefit plan using the Beneficiary model
        beneficiaries = Beneficiary.objects.filter(benefit_plan=self.benefit_plan)
        beneficiary_individuals = [beneficiary.individual for beneficiary in beneficiaries]
        self.assertIn(individual1, beneficiary_individuals)
        self.assertIn(individual2, beneficiary_individuals)

    @patch('individual.apps.IndividualConfig.enable_maker_checker_for_individual_upload', True)
    @patch('social_protection.apps.SocialProtectionConfig.enable_maker_checker_for_beneficiary_upload', True)
    def test_process_import_beneficiaries_workflow_with_all_valid_entries_with_maker_checker(self):
        # Update invalid entry in IndividualDataSource to valid data
        self.invalid_data_source.json_ext={
            "first_name": "Jane Workflow",
            "last_name": "Doe",
            "dob": "1982-01-01",
            "location_name": None,
        }
        self.invalid_data_source.save(user=self.user)

        process_import_beneficiaries_workflow(self.user_uuid, self.benefit_plan.uuid, self.upload_uuid)

        upload = IndividualDataSourceUpload.objects.get(id=self.upload_uuid)

        self.assertEqual(upload.status, "WAITING_FOR_VERIFICATION")
        self.assertEqual(upload.error, {})

        # Verify that individual IDs not yet assigned to data entries in IndividualDataSource
        data_entries = IndividualDataSource.objects.filter(upload_id=self.upload_uuid)
        for entry in data_entries:
            self.assertIsNone(entry.individual_id)
