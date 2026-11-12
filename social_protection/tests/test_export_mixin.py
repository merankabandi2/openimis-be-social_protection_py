import json
from types import SimpleNamespace
from unittest import mock

from django.test import SimpleTestCase

from social_protection import export_mixin
from social_protection.export_mixin import (
    EXPORT_HANDLERS,
    ExportableSocialProtectionQueryMixin,
    register_export_handler,
)


class _ExportQuery(ExportableSocialProtectionQueryMixin):
    exportable_fields = ['item']
    item = SimpleNamespace(filtering_args={'status'})
    resolved_queryset = None

    def resolve_item(self, info, **kwargs):
        return _ExportQuery.resolved_queryset


_ExportQuery.create_export_function('item')


class ExportHandlerRegistryTest(SimpleTestCase):
    """
    The exporter built by ExportableSocialProtectionQueryMixin dispatches to a handler
    registered for (file_format, field_name), and otherwise creates the default export
    in the requested file format.
    """

    def setUp(self):
        registry = mock.patch.dict(EXPORT_HANDLERS, clear=True)
        registry.start()
        self.addCleanup(registry.stop)

        self.queryset = mock.MagicMock(name='queryset')
        self.filtered_queryset = self.queryset.filter.return_value
        _ExportQuery.resolved_queryset = self.queryset
        self.user = object()
        self.info = SimpleNamespace(context=SimpleNamespace(user=self.user))

        create_csv_export = mock.patch.object(
            export_mixin.ExportableQueryModel, 'create_csv_export',
            return_value=SimpleNamespace(name='default-export'),
        )
        self.create_csv_export = create_csv_export.start()
        self.addCleanup(create_csv_export.stop)

    def _export(self, **kwargs):
        return _ExportQuery.resolve_item_export(
            None, self.info,
            fields=['code', 'group.code'],
            fields_columns=json.dumps({'code': 'Code'}),
            status='ACTIVE',
            **kwargs,
        )

    def test_registered_handler_receives_filtered_queryset(self):
        handler = mock.Mock(return_value=SimpleNamespace(name='handled-export'))
        register_export_handler('xlsx', 'item', handler)

        self.assertEqual(self._export(file_format='xlsx'), 'handled-export')

        self.queryset.filter.assert_called_once_with(status='ACTIVE')
        handler.assert_called_once_with(self.filtered_queryset, self.user, self.info)
        self.create_csv_export.assert_not_called()

    def test_registering_a_key_again_replaces_the_handler(self):
        first = mock.Mock(return_value=SimpleNamespace(name='first'))
        second = mock.Mock(return_value=SimpleNamespace(name='second'))
        register_export_handler('xlsx', 'item', first)
        register_export_handler('xlsx', 'item', second)

        self.assertEqual(self._export(file_format='xlsx'), 'second')
        first.assert_not_called()

    def test_handler_for_another_format_is_not_used(self):
        handler = mock.Mock()
        register_export_handler('xlsx', 'item', handler)

        self.assertEqual(self._export(file_format='csv'), 'default-export')

        handler.assert_not_called()
        self.assertEqual(self.create_csv_export.call_args.kwargs['file_format'], 'csv')

    def test_default_export_receives_requested_file_format(self):
        self.assertEqual(self._export(file_format='xlsx'), 'default-export')

        args, kwargs = self.create_csv_export.call_args
        self.assertEqual(args, (self.filtered_queryset, ['code', 'group__code'], self.user))
        self.assertEqual(kwargs['column_names'], {'code': 'Code'})
        self.assertEqual(kwargs['file_format'], 'xlsx')

    def test_default_export_is_csv_when_no_file_format_is_given(self):
        self._export()
        self.assertEqual(self.create_csv_export.call_args.kwargs['file_format'], 'csv')

    def test_default_export_is_csv_when_file_format_is_null(self):
        self._export(file_format=None)
        self.assertEqual(self.create_csv_export.call_args.kwargs['file_format'], 'csv')
