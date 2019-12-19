from django.contrib import admin, messages
from django.urls import reverse
from django.utils.html import format_html
from django.forms import forms

from .decorators import action_form
from .models import Observation, Run, Instance, Detection, UnresolvedDetection


class DetectionAdmin(admin.ModelAdmin):
    model = Detection
    show_change_link = True
    list_display = ('id', 'run', 'name', 'x', 'y', 'z', 'f_sum', 'ell_maj', 'ell_min', 'w20', 'w50')

    def get_queryset(self, request):
        qs = super(DetectionAdmin, self).get_queryset(request).select_related('run')
        return qs.filter(unresolved=False)

    def has_add_permission(self, request):
        return False

    def has_delete_permission(self, request, obj=None):
        return False

    def has_change_permission(self, request, obj=None):
        return False


class UnresolvedDetectionAdmin(admin.ModelAdmin):
    model = UnresolvedDetection
    show_change_link = True
    actions = ['check_action', 'resolve_action']

    def get_actions(self, request):
        if request.GET:
            return super(UnresolvedDetectionAdmin, self).get_actions(request)
        return None

    def get_list_display(self, request):
        if request.GET:
            return 'id', 'name', 'x', 'y', 'z', 'f_sum', 'ell_maj', 'ell_min', \
                   'w20', 'w50', 'moment0_image', 'spectrum_image'
        else:
            return 'id', 'run', 'name', 'x', 'y', 'z', 'f_sum', 'ell_maj', 'ell_min', 'w20', 'w50'

    def lookup_allowed(self, lookup, value):
        if lookup is None:
            return True
        elif lookup != 'run':
            return False
        return True

    def get_queryset(self, request):
        qs = super(UnresolvedDetectionAdmin, self).get_queryset(request).select_related('run')
        return qs.filter(unresolved=True)

    def has_add_permission(self, request):
        return False

    def has_delete_permission(self, request, obj=None):
        return False

    def has_change_permission(self, request, obj=None):
        return False

    class ResolveDetectionForm(forms.Form):
        title = 'One random unresolved detection below will marked as "resolved" and the rest deleted.'

    @action_form(ResolveDetectionForm)
    def resolve_action(self, request, queryset, form):
        pass

    resolve_action.short_description = 'Resolve Detections'

    def check_action(self, request, queryset):
        for index, detect_outer in enumerate(queryset):
            for detect_inner in queryset[index+1:]:
                if detect_outer.is_match(detect_inner):
                    sanity, msg = detect_outer.sanity_check(detect_inner)
                    if sanity is False:
                        messages.info(request, msg)

        return None

    check_action.short_description = 'Check Detections'


class DetectionAdminInline(admin.TabularInline):
    model = Detection
    show_change_link = True
    list_display = ('name', 'x', 'y', 'z', 'f_sum', 'ell_maj', 'ell_min', 'w20', 'w50')
    exclude = ['x_min', 'x_max', 'y_min', 'y_max', 'z_min', 'z_max', 'n_pix', 'f_min', 'f_max', 'rel', 'rms',
               'ell_pa', 'ell3s_maj', 'ell3s_min', 'ell3s_ps', 'err_x', 'err_y', 'err_z', 'err_f_sum',
               'ra', 'dec', 'freq', 'flag', 'unresolved', 'instance']
    readonly_fields = list_display
    fk_name = 'run'

    def get_queryset(self, request):
        qs = super(DetectionAdminInline, self).get_queryset(request)
        return qs.filter(unresolved=False)

    def has_add_permission(self, request):
        return False

    def has_delete_permission(self, request, obj=None):
        return False

    def has_change_permission(self, request, obj=None):
        return False


class UnresolvedDetectionAdminInline(admin.TabularInline):
    model = UnresolvedDetection
    show_change_link = True
    list_display = ('name', 'x', 'y', 'z', 'f_sum', 'ell_maj', 'ell_min', 'w20', 'w50')
    exclude = ['x_min', 'x_max', 'y_min', 'y_max', 'z_min', 'z_max', 'n_pix', 'f_min', 'f_max', 'rel', 'rms',
               'ell_pa', 'ell3s_maj', 'ell3s_min', 'ell3s_ps', 'err_x', 'err_y', 'err_z', 'err_f_sum',
               'ra', 'dec', 'freq', 'flag', 'unresolved', 'instance']
    readonly_fields = list_display
    ordering = ('x',)
    fk_name = 'run'

    def get_queryset(self, request):
        qs = super(UnresolvedDetectionAdminInline, self).get_queryset(request)
        return qs.filter(unresolved=True)

    def has_add_permission(self, request):
        return False

    def has_change_permission(self, request, obj=None):
        return False

    def has_delete_permission(self, request, obj=None):
        return False


class InstanceAdminInline(admin.TabularInline):
    model = Instance
    show_change_link = True
    list_display = ('filename', 'run_date', 'boundary')
    exclude = ['parameters']
    readonly_fields = list_display

    def has_add_permission(self, request):
        return False

    def has_delete_permission(self, request, obj=None):
        return False

    def has_change_permission(self, request, obj=None):
        return False


class InstanceAdmin(admin.ModelAdmin):
    model = Instance
    list_display = ('run', 'filename', 'run_date', 'boundary')
    exclude = ['parameters']
    readonly_fields = list_display
    #inlines = (DetectionAdminInline,)

    def has_add_permission(self, request):
        return False

    def has_delete_permission(self, request, obj=None):
        return False

    def has_change_permission(self, request, obj=None):
        return False


class RunAdmin(admin.ModelAdmin):
    model = Run
    list_display = ('name', 'sanity_thresholds', 'run_link')
    inlines = (UnresolvedDetectionAdminInline, DetectionAdminInline, InstanceAdminInline, )

    def run_link(self, obj):
        opts = self.model._meta
        url = reverse('admin:%s_%s_changelist' % (opts.app_label, 'unresolveddetection'))
        return format_html("<a href='%s?run=%s'>%s</a>" % (url, obj.id, 'View'))
    run_link.short_description = 'Unresolved Detections'

    def has_add_permission(self, request):
        return False

    def has_delete_permission(self, request, obj=None):
        return False

    def has_change_permission(self, request, obj=None):
        return False


class RunAdminInline(admin.TabularInline):
    model = Run
    show_change_link = True
    list_display = ['name', 'sanity_thresholds']
    fields = list_display
    readonly_fields = fields

    def has_add_permission(self, request):
        return False

    def has_delete_permission(self, request, obj=None):
        return False

    def has_change_permission(self, request, obj=None):
        return False


class ObservationAdmin(admin.ModelAdmin):
    list_display = ['name']
    inlines = (RunAdminInline,)

    def has_add_permission(self, request):
        return False

    def has_delete_permission(self, request, obj=None):
        return False

    def has_change_permission(self, request, obj=None):
        return False


admin.site.register(Observation, ObservationAdmin)
admin.site.register(Run, RunAdmin)
admin.site.register(Instance, InstanceAdmin)
admin.site.register(Detection, DetectionAdmin)
admin.site.register(UnresolvedDetection, UnresolvedDetectionAdmin)
