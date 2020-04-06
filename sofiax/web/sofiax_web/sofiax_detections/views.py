import io
import tarfile

from django.http import HttpResponse
from urllib.request import pathname2url
from .models import Products, Instance


def instance_products(request):
    if not request.user.is_authenticated:
        return HttpResponse('Unauthorized', status=401)

    instance_id = request.GET.get('id', None)
    if not instance_id:
        return HttpResponse('id does not exist.', status=400)

    try:
        instance_id = int(instance_id)
    except ValueError:
        return HttpResponse('id is not an integer.', status=400)

    instance = Instance.objects.filter(id=instance_id)
    if not instance:
        return HttpResponse('instance not found.', status=404)

    instance_name = instance[0].id
    run_name = instance[0].run.name
    name = f"{run_name}_{instance_name}"
    name = pathname2url(name.replace(' ', '_'))

    fh = io.BytesIO()
    with tarfile.open(fileobj=fh, mode='w:gz') as tar:
        if instance[0].reliability_plot is not None:
            info = tarfile.TarInfo(f'{name}_rel.eps')
            info.size = len(instance[0].reliability_plot)
            tar.addfile(info, io.BytesIO(initial_bytes=instance[0].reliability_plot))

        if instance[0].stdout is not None:
            info = tarfile.TarInfo(f'{name}_stdout.txt')
            info.size = len(instance[0].stdout)
            tar.addfile(info, io.BytesIO(initial_bytes=instance[0].stdout))

        if instance[0].stderr is not None:
            info = tarfile.TarInfo(f'{name}_stderr.txt')
            info.size = len(instance[0].stderr)
            tar.addfile(info, io.BytesIO(initial_bytes=instance[0].stderr))

    data = fh.getvalue()
    size = len(data)

    response = HttpResponse(data, content_type='application/x-tar')
    response['Content-Disposition'] = f'attachment; filename={name}.tar'
    response['Content-Length'] = size
    return response


def detection_products(request):
    if not request.user.is_authenticated:
        return HttpResponse('Unauthorized', status=401)

    detect_id = request.GET.get('id', None)
    if not detect_id:
        return HttpResponse('id does not exist.', status=400)

    try:
        detect_id = int(detect_id)
    except ValueError:
        return HttpResponse('id is not an integer.', status=400)

    product = Products.objects.filter(detection=detect_id).\
        select_related('detection',
                       'detection__instance',
                       'detection__run').only('detection__name', 'detection__instance__id', 'detection__run__name',
                                              'moment0', 'moment1', 'moment2', 'cube', 'mask', 'channels', 'spectrum')

    if not product:
        return HttpResponse('products not found.', status=404)

    detect_name = product[0].detection.name
    run_name = product[0].detection.run.name
    inst_name = product[0].detection.instance.id
    name = f"{run_name}_{inst_name}_{detect_name}"
    name = pathname2url(name.replace(' ', '_'))

    fh = io.BytesIO()
    with tarfile.open(fileobj=fh, mode='w:gz') as tar:
        info = tarfile.TarInfo(f'{name}_moment0.fits')
        info.size = len(product[0].moment0)
        tar.addfile(info, io.BytesIO(initial_bytes=product[0].moment0))

        info = tarfile.TarInfo(f'{name}_moment1.fits')
        info.size = len(product[0].moment1)
        tar.addfile(info, io.BytesIO(initial_bytes=product[0].moment1))

        info = tarfile.TarInfo(f'{name}_moment2.fits')
        info.size = len(product[0].moment2)
        tar.addfile(info, io.BytesIO(initial_bytes=product[0].moment2))

        info = tarfile.TarInfo(f'{name}_cube.fits')
        info.size = len(product[0].cube)
        tar.addfile(info, io.BytesIO(initial_bytes=product[0].cube))

        info = tarfile.TarInfo(f'{name}_mask.fits')
        info.size = len(product[0].mask)
        tar.addfile(info, io.BytesIO(initial_bytes=product[0].mask))

        info = tarfile.TarInfo(f'{name}_channels.fits')
        info.size = len(product[0].channels)
        tar.addfile(info, io.BytesIO(initial_bytes=product[0].channels))

        info = tarfile.TarInfo(f'{name}_spectrum.txt')
        info.size = len(product[0].spectrum)
        tar.addfile(info, io.BytesIO(initial_bytes=product[0].spectrum))

    data = fh.getvalue()
    size = len(data)

    response = HttpResponse(data, content_type='application/x-tar')
    response['Content-Disposition'] = f'attachment; filename={name}.tar'
    response['Content-Length'] = size
    return response
