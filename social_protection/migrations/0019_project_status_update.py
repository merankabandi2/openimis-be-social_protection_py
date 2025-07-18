# Generated by Django 4.2.20 on 2025-05-31 01:39

from django.db import migrations


def forwards(apps, schema_editor):
    Project = apps.get_model("social_protection", "Project")
    Project.objects.filter(status="IN PROGRESS").update(status="IN_PROGRESS")

def backwards(apps, schema_editor):
    Project = apps.get_model("social_protection", "Project")
    Project.objects.filter(status="IN_PROGRESS").update(status="IN PROGRESS")

class Migration(migrations.Migration):

    dependencies = [
        ('social_protection', '0018_alter_historicalproject_status_alter_project_status'),
    ]

    operations = [
        migrations.RunPython(forwards, backwards),
    ]
