import base64
import functools

from django.contrib.admin import helpers
from django.template.response import TemplateResponse
from django.http import HttpResponse
from django.contrib.auth import authenticate
from django.conf import settings


def action_form(form_class=None):
    def decorator(func):
        @functools.wraps(func)
        def wrapper(self, request, queryset):
            form = form_class()

            if 'confirm' in request.POST and request.POST:
                form = form_class(request.POST)
                if form.is_valid():
                    obj_count = func(self, request, queryset, form)
                    self.message_user(request, '%s objects updated' % obj_count)
                    return None

            context = dict(
                self.admin_site.each_context(request),
                title=form_class.title,
                action=func.__name__,
                opts=self.model._meta,
                queryset=queryset, form=form,
                action_checkbox_name=helpers.ACTION_CHECKBOX_NAME)

            return TemplateResponse(request, 'admin/form_action_confirmation.html', context)

        wrapper.short_description = form_class.title

        return wrapper
    return decorator


def basicauth(view):
    def wrap(request, *args, **kwargs):
        if request.user.is_authenticated:
            return view(request, *args, **kwargs)

        if 'HTTP_AUTHORIZATION' in request.META:
            auth = request.META['HTTP_AUTHORIZATION'].split()
            if len(auth) == 2:
                if auth[0].lower() == "basic":
                    uname, passwd = base64.b64decode(auth[1]).decode("utf8").split(':')
                    user = authenticate(username=uname, password=passwd)
                    if user is not None and user.is_active:
                        request.user = user
                        return view(request, *args, **kwargs)

        response = HttpResponse()
        response.status_code = 401
        response['WWW-Authenticate'] = 'Basic realm="Wallaby VO"'
        return response
    return wrap