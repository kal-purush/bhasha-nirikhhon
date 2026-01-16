# -*- coding: utf-8 -*-

import os
import subprocess
from ntpath import basename


class ImageTransformer(object):
    use_ras_transform = False
    use_center_surface = False
    use_cc_point = False
    converted_files_directory = "/home/pipeline/Downloads/work/git/bnm-recon-tools/python/converted_files/"
    created_files = []

    def __init__(self):
        if not os.path.exists(self.converted_files_directory):
            os.makedirs(self.converted_files_directory)

    def apply_transform(self, volume_path):
        if self.use_ras_transform:
            volume_new_file = open(self.converted_files_directory + 'ras' + basename(volume_path), "w+")
            volume_new_path = volume_new_file.name
            volume_new_file.close()

            try:
                x = subprocess.call(
                    ['mri_convert', '--out_orientation', 'RAS', '--out_type', 'nii', '--input_volume', volume_path,
                     '--output_volume', volume_new_path])
                print x
                self.created_files.append(volume_new_path)
                return volume_new_path

            except Exception:
                print "Error converting volume"

        else:
            return volume_path

    def center_surface(self, surface):
        if self.use_center_surface:
            surface_new_path = os.path.join(self.converted_files_directory, 'centered' + basename(surface))

            try:
                x = subprocess.call(['mris_convert', '--to-scanner', surface, surface_new_path])
                print x
                self.created_files.append(surface_new_path)
                return surface_new_path

            except Exception:
                print "Error converting surface"

        else:
            return surface

    def transform_single_volume(self, volume_path):
        return self.apply_transform(volume_path)

    def transform_2_volumes(self, background_path, overlay_path):
        return self.apply_transform(background_path), self.apply_transform(overlay_path)

    def transform_3_volumes(self, background_path, overlay_1_path, overlay_2_path):
        return self.apply_transform(background_path), self.apply_transform(overlay_1_path), self.apply_transform(
            overlay_2_path)

    def transform_volume_surfaces(self, background_path, surfaces_list):
        new_surfaces_list = [0 for _ in xrange(len(surfaces_list))]

        for i, surf in enumerate(surfaces_list):
            new_surfaces_list[i] = self.center_surface(os.path.expandvars(surf))

        return self.apply_transform(background_path), new_surfaces_list