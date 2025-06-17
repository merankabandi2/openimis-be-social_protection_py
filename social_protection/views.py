import logging
import json
import pandas as pd
import mimetypes
import os

from django.db.models import Q
from django.http import HttpResponse, StreamingHttpResponse
from django.utils.translation import gettext as _
from rest_framework import status
from rest_framework.decorators import api_view, permission_classes
from rest_framework.response import Response

from core.utils import DefaultStorageFileHandler
from core.views import check_user_rights
from individual.apps import IndividualConfig
from individual.models import IndividualDataSource
from social_protection.apps import SocialProtectionConfig
from social_protection.models import BenefitPlan, Beneficiary, GroupBeneficiary
from social_protection.services import BeneficiaryImportService, BeneficiaryExcelExportService
from workflow.services import WorkflowService

logger = logging.getLogger(__name__)


ALLOWED_EXTENSIONS = {".csv", ".xls", ".xlsx"}
ALLOWED_MIME_TYPES = {
    "text/csv",
    "application/vnd.ms-excel",
    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
}

mimetypes.add_type("application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", ".xlsx")
mimetypes.add_type("application/vnd.ms-excel", ".xls")


def is_valid_file(import_file):
    """ Validate file extension and MIME type """
    file_extension = os.path.splitext(import_file.name)[1].lower()
    if file_extension not in ALLOWED_EXTENSIONS:
        return False, _("Invalid file type. Allowed: .csv, .xls, .xlsx")

    file_mime_type, _ = mimetypes.guess_type(import_file.name)
    if not file_mime_type:
        return False, _("Could not determine file type")

    if file_mime_type not in ALLOWED_MIME_TYPES:
        return False, _(f"Invalid MIME type:") + f" {file_mime_type}"

    return True, None


def get_global_schema_fields(benefit_plan):
    schema = benefit_plan.beneficiary_data_schema if benefit_plan.beneficiary_data_schema \
        else json.loads(IndividualConfig.individual_schema)
    schema_properties = set(schema.get('properties', {}).keys())
    schema_properties.update(['recipient_info', 'individual_role', 'group_code'])
    return list(schema_properties)


@api_view(["GET"])
@permission_classes([check_user_rights(SocialProtectionConfig.gql_beneficiary_create_perms, )])
def download_template_benefit_plan_file(request):
    benefit_plan_uuid = request.GET.get('benefit_plan_uuid')
    benefit_plan = BenefitPlan.objects.filter(id=benefit_plan_uuid, is_deleted=False).first()
    try:
        base_fields = SocialProtectionConfig.beneficiary_base_fields
        extra_fields = get_global_schema_fields(benefit_plan)
        all_fields = base_fields + extra_fields
        template_df = pd.DataFrame(columns=all_fields)

        def stream_csv():
            output = template_df.to_csv(index=False)
            yield output.encode('utf-8')

        response = StreamingHttpResponse(
            stream_csv(), content_type='text/csv'
        )
        response['Content-Disposition'] = 'attachment; filename="beneficiary_upload_template.csv"'
        return response
    except Exception as exc:
        logger.error("Unexpected error while generating template file", exc_info=exc)
        return Response({'success': False, 'error': str(exc)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)


@api_view(["POST"])
@permission_classes([check_user_rights(IndividualConfig.gql_individual_create_perms, )])
def import_beneficiaries(request):
    import_file = None
    benefit_plan = None
    try:
        user = request.user
        import_file, workflow, benefit_plan, group_aggregation_column = _resolve_import_beneficiaries_args(request)

        is_valid, error_message = is_valid_file(import_file)
        if not is_valid:
            return Response({'success': False, 'error': error_message}, status=status.HTTP_400_BAD_REQUEST)

        _handle_file_upload(import_file, benefit_plan)
        result = BeneficiaryImportService(user).import_beneficiaries(
            import_file, benefit_plan, workflow, group_aggregation_column
        )
        if not result.get('success'):
            raise ValueError('{}: {}'.format(result.get("message"), result.get("details")))

        return Response(result)
    except ValueError as e:
        if import_file and benefit_plan:
            _remove_file(benefit_plan, import_file)
        logger.error("Error while uploading individuals", exc_info=e)
        return Response({'success': False, 'error': str(e)}, status=status.HTTP_400_BAD_REQUEST)
    except FileExistsError as e:
        logger.error("Error while saving file", exc_info=e)
        return Response({'success': False, 'error': str(e)}, status=status.HTTP_409_CONFLICT)
    except Exception as e:
        logger.error("Unexpected error while uploading individuals", exc_info=e)
        return Response({'success': False, 'error': str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)


@api_view(["POST"])
@permission_classes([check_user_rights(IndividualConfig.gql_individual_create_perms, )])
def validate_import_beneficiaries(request):
    try:
        user = request.user
        upload_id, individual_sources, benefit_plan = _resolve_validate_import_beneficiaries_args(request)

        result = BeneficiaryImportService(user).validate_import_beneficiaries(
            upload_id,
            individual_sources,
            benefit_plan
        )
        if not result.get('success'):
            raise ValueError('{}: {}'.format(result.get("message"), result.get("details")))

        # for now just return info if df valid
        return Response(result)
    except ValueError as e:
        logger.error("Error while validating individuals", exc_info=e)
        return Response({'success': False, 'error': str(e)}, status=400)
    except Exception as e:
        logger.error("Unexpected error while validating individuals", exc_info=e)
        return Response({'success': False, 'error': str(e)}, status=500)


@api_view(["POST"])
@permission_classes([check_user_rights(IndividualConfig.gql_individual_create_perms, )])
def create_task_with_importing_valid_items(request):
    try:
        user = request.user
        upload_id, benefit_plan = _resolve_create_task_with_importing_valid_items(request)
        BeneficiaryImportService(user).create_task_with_importing_valid_items(
            upload_id,
            benefit_plan
        )
        return Response({'success': True, 'error': None}, status=201)
    except ValueError as exc:
        logger.error("Error while sending callback to openIMIS", exc_info=exc)
        return Response({'success': False, 'error': str(exc)}, status=400)
    except Exception as exc:
        logger.error("Unexpected error while sending callback to openIMIS", exc_info=exc)
        return Response({'success': False, 'error': str(exc)}, status=500)


@api_view(["GET"])
@permission_classes([check_user_rights(IndividualConfig.gql_individual_search_perms, )])
def download_invalid_items(request):
    try:
        upload_id = request.query_params.get('upload_id')

        invalid_items = IndividualDataSource.objects.filter(
            Q(is_deleted=False) &
            Q(upload_id=upload_id) &
            ~Q(validations__validation_errors=[])
        )

        data_from_source = []
        for invalid_item in invalid_items:
            json_ext = invalid_item.json_ext
            invalid_item.json_ext["id"] = invalid_item.id
            invalid_item.json_ext["error"] = invalid_item.validations
            data_from_source.append(json_ext)

        recreated_df = pd.DataFrame(data_from_source)

        # Function to stream the DataFrame content as CSV
        def stream_csv():
            output = recreated_df.to_csv(index=False)
            yield output.encode('utf-8')

        # Create a streaming response with the CSV content
        response = StreamingHttpResponse(
            stream_csv(), content_type='text/csv'
        )
        response['Content-Disposition'] = 'attachment; filename="invalid_items.csv"'
        return response

    except ValueError as exc:
        # Handle errors gracefully
        logger.error("Error while fetching data", exc_info=exc)
        return Response({'success': False, 'error': str(exc)}, status=400)
    except Exception as exc:
        logger.error("Unexpected error", exc_info=exc)
        return Response({'success': False, 'error': str(exc)}, status=500)


@api_view(["GET"])
@permission_classes([check_user_rights(IndividualConfig.gql_individual_search_perms, )])
def download_beneficiary_upload(request):
    try:
        benefit_plan_id = request.query_params.get('benefit_plan_id')
        filename = request.query_params.get('filename')
        target_file_path = SocialProtectionConfig.get_beneficiary_upload_file_path(benefit_plan_id, filename)
        file_handler = DefaultStorageFileHandler(target_file_path)
        return file_handler.get_file_response_csv(filename)

    except ValueError as exc:
        logger.error("Error while fetching data", exc_info=exc)
        return Response({'success': False, 'error': str(exc)}, status=status.HTTP_400_BAD_REQUEST)
    except FileNotFoundError as exc:
        logger.error("Error while getting file", exc_info=exc)
        return Response({'success': False, 'error': str(exc)}, status=status.HTTP_404_NOT_FOUND)
    except Exception as exc:
        logger.error("Unexpected error", exc_info=exc)
        return Response({'success': False, 'error': str(exc)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)


@api_view(["POST"])
@permission_classes([check_user_rights(IndividualConfig.gql_individual_create_perms, )])
def synchronize_data_for_reporting(request):
    try:
        user = request.user
        upload_id, benefit_plan = _resolve_synchronize_data_for_reporting(request)
        BeneficiaryImportService(user).synchronize_data_for_reporting(
            upload_id,
            benefit_plan
        )
        return Response({'success': True, 'error': None}, status=201)
    except ValueError as exc:
        logger.error("Error while sending callback to openIMIS", exc_info=exc)
        return Response({'success': False, 'error': str(exc)}, status=400)
    except Exception as exc:
        logger.error("Unexpected error while sending callback to openIMIS", exc_info=exc)
        return Response({'success': False, 'error': str(exc)}, status=500)


def _resolve_import_beneficiaries_args(request):
    import_file = request.FILES.get('file')
    benefit_plan_uuid = request.POST.get('benefit_plan')
    workflow_name = request.POST.get('workflow_name')
    workflow_group = request.POST.get('workflow_group')
    group_aggregation_column = request.POST.get('group_aggregation_column')

    if not import_file:
        raise ValueError(f'Import file not provided')
    if not benefit_plan_uuid:
        raise ValueError(f'Benefit plan UUID not provided')
    if not workflow_name:
        raise ValueError(f'Workflow name not provided')
    if not workflow_group:
        raise ValueError(f'Workflow group not provided')
    benefit_plan = BenefitPlan.objects.filter(id=benefit_plan_uuid).first()
    if not benefit_plan:
        raise ValueError('Benefit Plan not found: {}'.format(benefit_plan_uuid))
    if (group_aggregation_column and
            benefit_plan.type != BenefitPlan.BenefitPlanType.GROUP_TYPE):
        raise ValueError(f'Group aggregation only for group type benefit plans')

    result = WorkflowService.get_workflows(workflow_name, workflow_group)
    if not result.get('success'):
        raise ValueError('{}: {}'.format(result.get("message"), result.get("details")))

    workflows = result.get('data', {}).get('workflows')

    if not workflows:
        raise ValueError('Workflow not found: group={} name={}'.format(workflow_group, workflow_name))
    if len(workflows) > 1:
        raise ValueError('Multiple workflows found: group={} name={}'.format(workflow_group, workflow_name))

    workflow = workflows[0]


    return import_file, workflow, benefit_plan, group_aggregation_column


def _resolve_validate_import_beneficiaries_args(request):
    benefit_plan_uuid = request.data.get('benefit_plan')
    upload_id = request.data.get('upload_id')

    benefit_plan = BenefitPlan.objects.filter(uuid=benefit_plan_uuid, is_deleted=False).first()
    individual_sources = IndividualDataSource.objects.filter(upload_id=upload_id)

    if not benefit_plan:
        raise ValueError('Benefit Plan not found: {}'.format(benefit_plan_uuid))

    return upload_id, individual_sources, benefit_plan


def _resolve_create_task_with_importing_valid_items(request):
    benefit_plan_uuid = request.data.get('benefit_plan')
    upload_id = request.data.get('upload_id')

    benefit_plan = BenefitPlan.objects.filter(uuid=benefit_plan_uuid, is_deleted=False).first()

    if not benefit_plan:
        raise ValueError('Benefit Plan not found: {}'.format(benefit_plan_uuid))

    return upload_id, benefit_plan


def _resolve_synchronize_data_for_reporting(request):
    benefit_plan_uuid = request.data.get('benefit_plan')
    upload_id = request.data.get('upload_id')

    benefit_plan = BenefitPlan.objects.filter(uuid=benefit_plan_uuid, is_deleted=False).first()

    if not benefit_plan:
        raise ValueError('Benefit Plan not found: {}'.format(benefit_plan_uuid))

    return upload_id, benefit_plan


def _handle_file_upload(file, benefit_plan):
    try:
        target_file_path = SocialProtectionConfig.get_beneficiary_upload_file_path(benefit_plan.id, file.name)
        file_handler = DefaultStorageFileHandler(target_file_path)
        file_handler.save_file(file)
    except FileExistsError as exc:
        raise exc


def _remove_file(benefit_plan, file):
    target_file_path = SocialProtectionConfig.get_beneficiary_upload_file_path(benefit_plan.id, file.name)
    file_handler = DefaultStorageFileHandler(target_file_path)
    file_handler.remove_file()


@api_view(["GET"])
@permission_classes([check_user_rights(SocialProtectionConfig.gql_beneficiary_search_perms, )])
def export_beneficiaries_excel(request):
    """Export group beneficiaries to Excel with specified format including photo URLs"""
    try:
        # Get filters from query params
        benefit_plan_id = request.query_params.get('benefit_plan_id')
        status_filter = request.query_params.get('status')
        
        # Build queryset for GroupBeneficiary
        queryset = GroupBeneficiary.objects.filter(is_deleted=False)
        
        if benefit_plan_id:
            queryset = queryset.filter(benefit_plan_id=benefit_plan_id)
        
        if status_filter:
            queryset = queryset.filter(status=status_filter)
        
        # Add ordering to prevent pagination warning
        queryset = queryset.order_by('-date_created', 'id')
        
        # Get base URL from request
        base_url = request.build_absolute_uri('/').rstrip('/')
        
        # Create export service
        export_service = BeneficiaryExcelExportService(request.user, base_url)
        
        # Generate Excel file
        excel_file = export_service.export_group_beneficiaries_to_excel(
            queryset, 
            filename=f'beneficiaries_{status_filter or "all"}.xlsx'
        )
        
        # Create response
        response = HttpResponse(
            excel_file.read(),
            content_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
        )
        response['Content-Disposition'] = f'attachment; filename="beneficiaries_{status_filter or "all"}.xlsx"'
        
        return response
        
    except Exception as exc:
        logger.error("Error while exporting group beneficiaries to Excel", exc_info=exc)
        return Response({'success': False, 'error': str(exc)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
