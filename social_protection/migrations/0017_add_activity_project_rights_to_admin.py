from django.db import migrations
from core.utils import insert_role_right_for_system, remove_role_right_for_system

activity_rights = [208001, 208002, 208003, 208004]
project_rights = [209001, 209002, 209003, 209004]
imis_administrator_system = 64


def add_rights(apps, schema_editor):
    for right_id in activity_rights + project_rights:
        insert_role_right_for_system(imis_administrator_system, right_id, apps)


def remove_rights(apps, schema_editor):
    for right_id in activity_rights + project_rights:
        remove_role_right_for_system(imis_administrator_system, right_id, apps)


class Migration(migrations.Migration):

    dependencies = [
        ('social_protection', '0016_project_historicalproject'),
    ]

    operations = [
        migrations.RunPython(add_rights, remove_rights),
    ]
