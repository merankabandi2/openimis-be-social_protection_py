import datetime
import json
import logging
import types
import uuid
from typing import Dict, List, Callable

import graphene
import pandas as pd
from django.db import models
from graphene.types.generic import GenericScalar
from pandas import DataFrame

from core import fields
from core.custom_filters import CustomFilterWizardStorage
from core.models import ExportableQueryModel
from graphql.utils.ast_to_dict import ast_to_dict
from core.gql.export_mixin import ExportableQueryMixin

logger = logging.getLogger(__file__)


class ExportableSocialProtectionQueryMixin(ExportableQueryMixin):

    @classmethod
    def create_export_function(cls, field_name):
        new_function_name = f"resolve_{field_name}_export"
        default_resolve = getattr(cls, F"resolve_{field_name}", None)

        if not default_resolve:
            raise AttributeError(
                f"Query {cls} doesn't provide resolve function for {field_name}. "
                f"CSV export cannot be created")

        def exporter(cls, self, info, **kwargs):
            custom_filters = kwargs.pop("customFilters", None)
            export_fields = [cls._adjust_notation(f) for f in kwargs.pop('fields')]
            fields_mapping = json.loads(kwargs.pop('fields_columns'))
            file_format = kwargs.pop('file_format', 'csv')

            source_field = getattr(cls, field_name)
            filter_kwargs = {k: v for k, v in kwargs.items() if k in source_field.filtering_args}

            qs = default_resolve(None, info, **kwargs)
            qs = qs.filter(**filter_kwargs)
            qs = cls.__append_custom_filters(custom_filters, qs, fields_mapping)
            # Handle special Excel export for group beneficiaries with photos
            if file_format == 'xlsx' and field_name == 'group_beneficiary':
                from .services import BeneficiaryExcelExportService
                import tempfile
                import os
                from django.core.files.storage import default_storage
                from django.core.files.base import ContentFile
                
                base_url = info.context.build_absolute_uri('/').rstrip('/')
                export_service = BeneficiaryExcelExportService(info.context.user, base_url)
                excel_file = export_service.export_group_beneficiaries_to_excel(qs)
                
                # Generate a unique filename
                timestamp = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
                filename = f'beneficiaries_export_{timestamp}.xlsx'
                
                # Get the file content
                excel_content = excel_file.getvalue()
                
                # Save the file using Django's storage system
                file_content = ContentFile(excel_content)
                saved_path = default_storage.save(f'exports/{filename}', file_content)
                
                # Create export record - ExportableQueryModel expects content as a file
                export_obj = ExportableQueryModel(
                    name=filename,
                    model='GroupBeneficiary',
                    content=saved_path,  # The file path saved by Django storage
                    user=info.context.user,
                    sql_query=str(qs.query),  # Store the query for reference
                    file_format='xlsx'
                )
                export_obj.save()
                
                return export_obj.name
            else:
                export_file = ExportableQueryModel\
                    .create_csv_export(qs, export_fields, info.context.user, column_names=fields_mapping,
                                       patches=cls.get_patches_for_field(field_name))
                return export_file.name

        setattr(cls, new_function_name, types.MethodType(exporter, cls))

    @classmethod
    def __append_custom_filters(cls, custom_filters, queryset, fields_mapping):
        if custom_filters:
            module_name = cls.get_module_name()
            object_type = cls.get_object_type()
            related_field = cls.get_related_field()
            if "group__id" in fields_mapping:
                queryset = CustomFilterWizardStorage.build_custom_filters_queryset(
                    "individual",
                    "GroupIndividual",
                    custom_filters,
                    queryset,
                    relation="group"
                )
            else:
                queryset = CustomFilterWizardStorage.build_custom_filters_queryset(
                    module_name,
                    object_type,
                    custom_filters,
                    queryset,
                    relation=related_field
                )
        return queryset
