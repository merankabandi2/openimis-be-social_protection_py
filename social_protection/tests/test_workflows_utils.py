from django.test import TestCase
from unittest.mock import patch, MagicMock
from core.test_helpers import create_test_interactive_user
from social_protection.workflows.utils import SqlProcedurePythonWorkflow, PythonWorkflowHandlerException
import pandas as pd
import json
import uuid
from social_protection.models import BenefitPlan

class TestBasePythonWorkflowExecutor(TestCase):

    def setUp(self):
        self.user = create_test_interactive_user(username="admin")
        self.upload_id = uuid.uuid4()
        
        self.plan_schema = {"properties": {}}

        # create BenefitPlan with the required fields and save with username
        self.benefit_plan = BenefitPlan(
            name="Test Benefit Plan",
            description='A test benefit plan',
            code='TESTPlan',
            max_beneficiaries=1000,
            beneficiary_data_schema=self.plan_schema
            # Add other required fields here
        )
        self.benefit_plan.save(username=self.user.username)

        self.mock_load_dataframe = patch('social_protection.workflows.utils.load_dataframe').start()
        self.mock_load_dataframe.return_value = pd.DataFrame()

        self.executor = SqlProcedurePythonWorkflow(self.benefit_plan.uuid, self.upload_id, self.user.id)

    def tearDown(self):
        patch.stopall()

    def test_validate_dataframe_headers_valid(self):
        self.executor.df = pd.DataFrame(columns=['first_name', 'last_name', 'dob', 'id', 'location_name', 'location_code'])
        try:
            self.executor.validate_dataframe_headers()
        except PythonWorkflowHandlerException:
            self.fail("validate_dataframe_headers() raised PythonWorkflowHandlerException unexpectedly!")

    def test_validate_dataframe_headers_missing_required_fields(self):
        # DataFrame missing required 'first_name' column
        self.executor.df = pd.DataFrame(columns=['last_name', 'dob', 'id', 'location_name', 'location_code'])

        with self.assertRaises(PythonWorkflowHandlerException) as cm:
            self.executor.validate_dataframe_headers()

        self.assertIn("Uploaded beneficiaries missing essential header: first_name", str(cm.exception))

    def test_validate_dataframe_headers_invalid_headers(self):
        # DataFrame with an invalid column
        self.executor.df = pd.DataFrame(columns=['first_name', 'last_name', 'dob', 'id', 'location_name', 'location_code', 'unexpected_column'])

        with self.assertRaises(PythonWorkflowHandlerException) as cm:
            self.executor.validate_dataframe_headers()

        self.assertIn("Uploaded beneficiaries contains invalid columns: {'unexpected_column'}", str(cm.exception))

    def test_validate_dataframe_headers_update_missing_id(self):
        # DataFrame missing 'ID' when is_update=True
        columns = ['first_name', 'last_name', 'dob', 'id', 'location_name', 'location_code']
        self.executor.df = pd.DataFrame(columns=columns)

        with self.assertRaises(PythonWorkflowHandlerException) as cm:
            self.executor.validate_dataframe_headers(is_update=True)

        self.assertIn("Uploaded beneficiaries missing essential header: ID", str(cm.exception))

        # adding ID should pass the validation
        columns.append('ID')
        self.executor.df = pd.DataFrame(columns=columns)
        try:
            self.executor.validate_dataframe_headers(is_update=True)
        except PythonWorkflowHandlerException:
            self.fail("validate_dataframe_headers() raised PythonWorkflowHandlerException unexpectedly!")

