from django.db import models

import math
import cv2
import numpy as np
import binascii

import matplotlib
matplotlib.use('Agg')

from PIL import Image
import matplotlib.pyplot as plt
from io import BytesIO, StringIO
from astropy.io import fits
from django.utils.safestring import mark_safe


class Detection(models.Model):
    id = models.BigAutoField(primary_key=True)
    instance = models.ForeignKey('Instance', models.DO_NOTHING)
    run = models.ForeignKey('Run', models.DO_NOTHING)
    name = models.TextField(blank=True, null=True)
    x = models.DecimalField(max_digits=65535, decimal_places=12)
    y = models.DecimalField(max_digits=65535, decimal_places=12)
    z = models.DecimalField(max_digits=65535, decimal_places=12)
    x_min = models.IntegerField(blank=True, null=True)
    x_max = models.IntegerField(blank=True, null=True)
    y_min = models.IntegerField(blank=True, null=True)
    y_max = models.IntegerField(blank=True, null=True)
    z_min = models.IntegerField(blank=True, null=True)
    z_max = models.IntegerField(blank=True, null=True)
    n_pix = models.IntegerField(blank=True, null=True)
    f_min = models.DecimalField(max_digits=65535, decimal_places=12, blank=True, null=True)
    f_max = models.DecimalField(max_digits=65535, decimal_places=12, blank=True, null=True)
    f_sum = models.DecimalField(max_digits=65535, decimal_places=12, blank=True, null=True)
    rel = models.DecimalField(max_digits=65535, decimal_places=12, blank=True, null=True)
    rms = models.DecimalField(max_digits=65535, decimal_places=12, blank=True, null=True)
    w20 = models.DecimalField(max_digits=65535, decimal_places=12, blank=True, null=True)
    w50 = models.DecimalField(max_digits=65535, decimal_places=12, blank=True, null=True)
    ell_maj = models.DecimalField(max_digits=65535, decimal_places=12, blank=True, null=True)
    ell_min = models.DecimalField(max_digits=65535, decimal_places=12, blank=True, null=True)
    ell_pa = models.DecimalField(max_digits=65535, decimal_places=12, blank=True, null=True)
    ell3s_maj = models.DecimalField(max_digits=65535, decimal_places=12, blank=True, null=True)
    ell3s_min = models.DecimalField(max_digits=65535, decimal_places=12, blank=True, null=True)
    ell3s_ps = models.DecimalField(max_digits=65535, decimal_places=12, blank=True, null=True)
    err_x = models.DecimalField(max_digits=65535, decimal_places=12, blank=True, null=True)
    err_y = models.DecimalField(max_digits=65535, decimal_places=12, blank=True, null=True)
    err_z = models.DecimalField(max_digits=65535, decimal_places=12, blank=True, null=True)
    err_f_sum = models.DecimalField(max_digits=65535, decimal_places=12, blank=True, null=True)
    ra = models.DecimalField(max_digits=65535, decimal_places=12, blank=True, null=True)
    dec = models.DecimalField(max_digits=65535, decimal_places=12, blank=True, null=True)
    freq = models.DecimalField(max_digits=65535, decimal_places=12, blank=True, null=True)
    flag = models.IntegerField(blank=True, null=True)
    unresolved = models.BooleanField()

    def __str__(self):
        return self.name

    def sanity_check(self, detect):
        if self.id == detect.id:
            raise ValueError('Same detection.')

        if self.run.id != detect.run.id:
            raise ValueError('Detection belongs to different run.')

        sanity_thresholds = self.run.sanity_thresholds

        f1 = self.f_sum
        f2 = detect.f_sum
        flux_threshold = sanity_thresholds['flux']
        diff = abs(f1 - f2) * 100 / ((abs(f2) + abs(f2)) / 2)
        # gone beyond the % tolerance
        if diff > flux_threshold:
            return False, f"Detections: {self.id}, {detect.id} Var: flux {round(diff, 2)}% > {flux_threshold}%"

        min_extent, max_extent = sanity_thresholds['spatial_extent']
        max1 = self.ell_maj
        max2 = detect.ell_maj
        min1 = self.ell_min
        min2 = detect.ell_min

        max_diff = abs(max1 - max2) * 100 / ((abs(max1) + abs(max2)) / 2)
        min_diff = abs(min1 - min2) * 100 / ((abs(min1) + abs(min2)) / 2)
        if max_diff > max_extent:
            return False, f"Detections: {self.id}, {detect.id} Var: ell_maj " \
                          f"Check: {round(max_diff, 2)}% > {max_extent}%"

        if min_diff > min_extent:
            return False, f"Detections: {self.id}, {detect.id} Var: ell_min " \
                          f"Check: {round(min_diff, 2)}% > {min_extent}%"

        min_extent, max_extent = sanity_thresholds['spectral_extent']
        max1 = self.w20
        max2 = detect.w20
        min1 = self.w50
        min2 = detect.w50
        max_diff = abs(max1 - max2) * 100 / ((abs(max1) + abs(max2)) / 2)
        min_diff = abs(min1 - min2) * 100 / ((abs(min1) + abs(min2)) / 2)
        if max_diff > max_extent:
            return False, f"Detections: {self.id}, {detect.id} Var: w20 " \
                          f"Check: {round(max_diff, 2)}% > {max_extent}%"

        if min_diff > min_extent:
            return False, f"Detections: {self.id}, {detect.id} Var: w50 " \
                          f"Check: {round(min_diff, 2)}% > {min_extent}%"

        return True, None

    def is_match(self, detect):
        if self.id == detect.id:
            raise ValueError('Same detection.')

        if self.run.id != detect.run.id:
            raise ValueError('Detection belongs to different run.')

        if self.x == detect.x and self.y == detect.y and self.z == detect.z:
            return True

        d_space = math.sqrt((self.x - detect.x) ** 2 + (self.y - detect.y) ** 2)
        d_space_err = math.sqrt((self.x - detect.x) ** 2 * (self.err_x ** 2 + detect.err_x ** 2) +
                                (self.y - detect.y) ** 2 * (self.err_y ** 2 + detect.err_y ** 2)) / \
                      ((self.x - detect.x) ** 2 + (self.y - detect.y) ** 2)
        d_spec = abs(self.z - detect.z)
        d_spec_err = math.sqrt(self.err_z ** 2 + detect.err_z ** 2)

        return d_space <= 3 * d_space_err and d_spec <= 3 * d_spec_err

    def spectrum_image(self):
        product = self.products_set.only('spectrum')
        if not product:
            return None

        x = []
        y = []
        with StringIO(product[0].spectrum.tobytes().decode('ascii')) as f:
            for line in f:
                li = line.strip()
                if not li.startswith("#"):
                    data = line.split()
                    x.append(float(data[1]))
                    y.append(float(data[2]))

        x = np.array(x)
        y = np.array(y)

        fig, ax = plt.subplots(nrows=1, ncols=1)
        fig.set_size_inches(2, 1)
        ax.plot(x, y, linewidth=1)
        ax.axhline(y.max() * .5, linewidth=1, color='r', alpha=0.5)
        ax.axhline(y.max() * .2, linewidth=1, color='r', alpha=0.5)
        ax.set_yticklabels([])
        ax.set_xticklabels([])

        with BytesIO() as image_data:
            fig.savefig(image_data, format='png')
            base_img = binascii.b2a_base64(image_data.getvalue()).decode()
            img_src = f'<img src=\"data:image/png;base64,{base_img}\">'
            plt.close(fig)
            return mark_safe(img_src)

    def moment0_image(self):
        product = self.products_set.only('moment0')
        if not product:
            return None

        with fits.open(BytesIO(product[0].moment0)) as hdu:
            data = hdu[0].data
            img = 255 * ((data - data.min()) / data.ptp())
            img = img.astype(np.uint8)
            img = cv2.applyColorMap(img, cv2.COLORMAP_HSV)
            img = Image.fromarray(img, 'RGB')
            img = img.resize((hdu[0].header['NAXIS1']*2, hdu[0].header['NAXIS2']*2), Image.BICUBIC)
            with BytesIO() as image_file:
                img.save(image_file, format='PNG')
                image_data = image_file.getvalue()
                base_img = binascii.b2a_base64(image_data).decode()
                img_src = f'<img src=\"data:image/png;base64,{base_img}\">'
                return mark_safe(img_src)

    class Meta:
        managed = False
        db_table = 'Detection'
        ordering = ("x",)
        unique_together = (('ra', 'dec', 'freq', 'instance', 'run'),)


class UnresolvedDetection(Detection):

    class Meta:
        proxy = True


class Instance(models.Model):
    id = models.BigAutoField(primary_key=True)
    run = models.ForeignKey('Run', models.DO_NOTHING)
    filename = models.TextField()
    boundary = models.TextField()  # This field type is a guess.
    run_date = models.DateTimeField()
    flag_log = models.BinaryField(blank=True, null=True)
    reliability_plot = models.BinaryField(blank=True, null=True)
    log = models.BinaryField(blank=True, null=True)
    parameters = models.TextField()  # This field type is a guess.

    def __unicode__(self):
        return f"{str(self.id)}"

    def __str__(self):
        return f"{str(self.id)}"

    class Meta:
        managed = False
        db_table = 'Instance'
        unique_together = (('run', 'filename', 'boundary'),)


class Products(models.Model):
    id = models.BigAutoField(primary_key=True)
    detection = models.ForeignKey(Detection, models.DO_NOTHING)
    cube = models.BinaryField(blank=True, null=True)
    mask = models.BinaryField(blank=True, null=True)
    moment0 = models.BinaryField(blank=True, null=True)
    moment1 = models.BinaryField(blank=True, null=True)
    moment2 = models.BinaryField(blank=True, null=True)
    channels = models.BinaryField(blank=True, null=True)
    spectrum = models.BinaryField(blank=True, null=True)

    class Meta:
        managed = False
        db_table = 'Products'
        unique_together = (('detection',),)


class Run(models.Model):
    id = models.BigAutoField(primary_key=True)
    name = models.TextField()
    sanity_thresholds = models.TextField()  # This field type is a guess.

    def __str__(self):
        return f"{self.name}"

    class Meta:
        managed = False
        db_table = 'Run'
        unique_together = (('name', 'sanity_thresholds'),)


class SpatialRefSys(models.Model):
    srid = models.IntegerField(primary_key=True)
    auth_name = models.CharField(max_length=256, blank=True, null=True)
    auth_srid = models.IntegerField(blank=True, null=True)
    srtext = models.CharField(max_length=2048, blank=True, null=True)
    proj4text = models.CharField(max_length=2048, blank=True, null=True)

    class Meta:
        managed = False
        db_table = 'spatial_ref_sys'
