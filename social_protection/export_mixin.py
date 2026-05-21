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


# Registry of custom (file_format, field_name) -> handler callables.
# Downstream modules (e.g. country-specific extensions) call
# ``register_export_handler`` from their AppConfig.ready() to override the
# default CSV/XLSX export for a specific (format, field) pair.
#
# Handler signature: ``handler(queryset, user, info) -> ExportableQueryModel``
# The returned ExportableQueryModel must already be ``.save()``-d; its ``.name``
# is what the GQL mutation returns to the client.
EXPORT_HANDLERS: Dict[tuple, Callable] = {}


def register_export_handler(file_format: str, field_name: str, handler: Callable) -> None:
    """Register a downstream override for a (file_format, field_name) export.

    Idempotent: re-registering the same key overwrites the previous handler
    (so app-reload during dev doesn't accumulate stale entries).
    """
    EXPORT_HANDLERS[(file_format, field_name)] = handler


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
            # Dispatch to a downstream handler if one was registered for this
            # (file_format, field_name) pair; otherwise fall through to the
            # default CSV export.
            handler = EXPORT_HANDLERS.get((file_format, field_name))
            if handler:
                export_obj = handler(qs, info.context.user, info)
                return export_obj.name
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
                    queryset
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
