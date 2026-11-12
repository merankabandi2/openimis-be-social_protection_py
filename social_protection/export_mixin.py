import json
import logging
import types
from typing import Callable, Dict


from core.custom_filters import CustomFilterWizardStorage
from core.models import ExportableQueryModel
from core.gql.export_mixin import ExportableQueryMixin

logger = logging.getLogger(__file__)


# Export handlers keyed by (file_format, field_name). A module registers one with
# ``register_export_handler`` from its AppConfig.ready() to replace the default
# export of that query field in that file format.
#
# Handler signature: ``handler(queryset, user, info) -> ExportableQueryModel``.
# The handler saves the returned ExportableQueryModel; the resolver returns its ``name``.
EXPORT_HANDLERS: Dict[tuple, Callable] = {}


def register_export_handler(file_format: str, field_name: str, handler: Callable) -> None:
    """Register the export handler for a (file_format, field_name) pair.

    Registering the same pair again replaces the previous handler.
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
            file_format = kwargs.pop('file_format', None) or 'csv'

            source_field = getattr(cls, field_name)
            filter_kwargs = {k: v for k, v in kwargs.items() if k in source_field.filtering_args}

            qs = default_resolve(None, info, **kwargs)
            qs = qs.filter(**filter_kwargs)
            qs = cls.__append_custom_filters(custom_filters, qs, fields_mapping)
            handler = EXPORT_HANDLERS.get((file_format, field_name))
            if handler:
                export_obj = handler(qs, info.context.user, info)
                return export_obj.name
            export_file = ExportableQueryModel\
                .create_csv_export(qs, export_fields, info.context.user, column_names=fields_mapping,
                                   patches=cls.get_patches_for_field(field_name), file_format=file_format)

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
