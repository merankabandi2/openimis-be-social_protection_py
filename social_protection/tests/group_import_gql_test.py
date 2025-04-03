from unittest import mock
import graphene
import random 
import uuid 
import string 
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
        create_group_with_individual, add_group_to_benefit_plan, create_individual, add_individual_to_group
from social_protection.services import GroupBeneficiaryService
from social_protection.models import IndividualDataSourceUpload
import json
from django.core.files.uploadedfile import SimpleUploadedFile
from tasks_management.models import Task, TaskExecutor, TaskGroup
from tasks_management.services import TaskService, TaskExecutorService, TaskGroupService
from tasks_management.tests.data import TaskDataMixin
from core.test_helpers import LogInHelper

class GroupBeneficiaryImportGQLTest(openIMISGraphQLTestCase, TaskDataMixin):
    schema = Schema(query=sp_schema.Query)


    class AnonymousUserContext:
        user = mock.Mock(is_anonymous=True)

    @classmethod
    def setUpClass(cls):
        super(GroupBeneficiaryImportGQLTest, cls).setUpClass()
        cls.user = User.objects.filter(username='admin', i_user__isnull=False).first()
        if not cls.user:
            cls.user = create_test_interactive_user(username='admin')
            
        cls.user_token = BaseTestContext(user=cls.user).get_jwt()
        cls.benefit_plan = create_benefit_plan(cls.user.username, payload_override={
            'code': 'GGQLTest',
            'type': 'GROUP',
            'beneficiary_data_schema': """{"$id": "https://example.com/beneficiares.schema.json", "type": "object", "title": "Record of beneficiares", "$schema": "http://json-schema.org/draft-04/schema#", "properties": {"email": {"type": "string", "description": "email address to contact with beneficiary", "validationCalculation": {"name": "EmailValidationStrategy"}}, "groupId": {"type": "string", "description": "Group categorization"}, "able_bodied": {"type": "boolean", "description": "Flag determining whether someone is able bodied or not"}, "national_id": {"type": "string", "uniqueness": true, "description": "national id"}, "educated_level": {"type": "string", "description": "The level of person when it comes to the school/education/studies"}, "recipient_info": {"type": "integer", "description": "main or not recipient_info"}, "chronic_illness": {"type": "boolean", "description": "Flag determining whether someone has such kind of illness or not"}, "national_id_type": {"type": "string", "description": "A type of national id"}, "number_of_elderly": {"type": "integer", "description": "Number of elderly"}, "number_of_children": {"type": "integer", "description": "Number of children"}, "beneficiary_data_source": {"type": "string", "description": "The source from where such beneficiary comes"}}, "description": "This document records the details beneficiares"}"""  
        })
        
        cls.task_executor = LogInHelper().get_or_create_user_api(
                username='TaskExecutor')
        cls.init_data()
        
        obj = TaskGroupService(cls.user).create({
            **cls.task_group_add_payload_any,
            "user_ids": [cls.task_executor.id, cls.user.id]
        })
        
        group_id = obj.get("data")["id"]
        cls.task_group = TaskGroup.objects.get(id=group_id)
        cls.task_group_service = TaskGroupService(cls.user)
        cls.task_service = TaskService(cls.user)

    def test_import_beneficiaries(self):
        # Prepare the file to be uploaded
        csv_content = (
            "first_name,last_name,dob,email,able_bodied,national_id,educated_level,national_id_type,number_of_elderly,number_of_children,groupId,recipient_info\n"
            "NTestPerson1AA,TestPerson1AA,1995-07-13,maixl21@test.com,False,111A11122,basic education,National ID Card,20,4,\"g12\",\"1\"\n"
            "NTestPerson1BB,TestPerson1BB,1995-07-13,m2xail2@test.com,False,123A12312S,basic education,National ID Card,20,4,\"g12\",\"0\"\n"
        )
        
        filename =  F"{''.join(random.choices(string.ascii_uppercase + string.digits, k=10))}.csv"
        csv_file = SimpleUploadedFile(
            filename,
            csv_content.encode("utf-8"),
            content_type="text/csv"
        )

        # Prepare the JWT token for the request
        headers = {
            'Authorization': f'Bearer {self.user_token}'
        }

        # Prepare the payload
        data = {
            "file": csv_file,
            "benefit_plan": str(self.benefit_plan.id),
            "workflow_name": "Python Beneficiaries Upload",
            "workflow_group": "socialProtection",
            "group_aggregation_column": "groupId",
        }

        # Send the POST request to the import endpoint
        response = self.client.post(
            "/api/social_protection/import_beneficiaries/",
            data,
            format='multipart',
            headers=headers,
        )

        # Assert the response status code
        self.assertEqual(response.status_code, 200)
        content = response.data
        self.assertEqual(content['success'], True)
        
        upload_uuid = content['data']['upload_uuid']
        
        upload = IndividualDataSourceUpload.objects.get(id=upload_uuid)
        self.assertEqual(
          upload.status, IndividualDataSourceUpload.Status.WAITING_FOR_VERIFICATION,
          F"Invalid upload status, should be waiting for verification, is {upload.status}. Error list: \n{upload.error}"
          )
        
        pending_task = Task.objects.all().order_by('date_created').last()
        self.assertEqual(pending_task.json_ext['source_name'], filename, "Task for approving upload of group not found")
        
        
        mut_id_task_update = uuid.uuid4()
        raw_input = F"""clientMutationId: "{mut_id_task_update}"\n id: \"{pending_task.uuid}\"\n  status: ACCEPTED\n  taskGroupId: \"{self.task_group.uuid}\"\n       """
        content=self.send_mutation("updateTask", raw_input, self.user_token, raw=True)        
        self.assertEqual(content['data']['mutationLogs']['edges'][0]['node']['status'], 2, "Fail during Task Group assingnment")
        
        # self.task_service.complete_task()
        
        input_param = {}
        mut_id = uuid.uuid4()
        raw_input = F"""clientMutationId: "{mut_id}"\n        id: "{pending_task.uuid}"\n        businessStatus: "{{\\"{self.user.id}\\":\\"APPROVED\\"}}"\n"""
        content=self.send_mutation("resolveTask", raw_input, self.user_token, raw=True)        
        self.assertEqual(content['data']['mutationLogs']['edges'][0]['node']['status'], 2, "Fail during Task resolve")
        
        pending_task.refresh_from_db()
        upload.refresh_from_db()
        
        
        ## At the moment Test Fail on Beneficiary Synchronization in self._synchronize_beneficiary(benefit_plan, upload_id) in 
        # openimis-be-social_protection_py/social_protection/services.py 
        self.assertEqual(
          upload.status, IndividualDataSourceUpload.Status.SUCCESS,
          F"Failure during individuals record upload. Expected success, actual status: {upload.status}. \nErrors: {upload.error}"
        )
        
        # TODO: Add assertion for the group creation 
        # TODO: Add assertion for beneficiary creation 