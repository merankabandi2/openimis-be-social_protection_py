from django.contrib import admin
from .models import Activity

@admin.register(Activity)
class ActivityAdmin(admin.ModelAdmin):
    list_display = [
        'id',
        'name',
        'version',
        'is_deleted',
        'user_updated',
        'date_updated',
    ]
    readonly_fields = [
        'user_created', 'date_created', 'user_updated', 'date_updated', 'version'
    ]
    exclude = ['date_valid_from', 'date_valid_to', 'json_ext', 'replacement_uuid']
    search_fields = ['name']

    def save_model(self, request, obj, form, change):
        obj.save(user=request.user)
