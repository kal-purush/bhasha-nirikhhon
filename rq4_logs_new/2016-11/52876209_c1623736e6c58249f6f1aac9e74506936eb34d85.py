
"""
I/O access to FreeSurfer databases.

While we currently only use FS, this is a starting point for a DAO API which is ideally software agnostic.

"""

import os
import glob
import scipy.io
import gdist
import nibabel.freesurfer
from .surf import convert_fs_to_brain_visa

# TODO validate / enum for known annotations and surface types
# TODO test suite against fsaverage subject or our own

class FreeSurferIO(object):
    "Implements I/O against FreeSurfer databases (i.e. $self.subjects_dir/$self.subject/*)."

    def __init__(self, subject, subjects_dir):
       self.subject = subject
       self.subjects_dir = subjects_dir

    @classmethod
    def from_env_vars(cls):
        "Convenience constructor from environment variables self.subjects_dir and self.subject."
        return cls(subject=os.environ['self.subject'], subjects_dir=os.environ['self.subjects_dir'])

    def annot_path(self, hemi, annot_name):
        "Build path to a given annotation for a given hemisphere."
        annot_fname = '%s.%s.annot' % (hemi, annot_name)
        annot_path = os.path.join(self.subjects_dir, self.subject, 'label', annot_fname)
        return annot_path

    def read_annot(self, hemi, annot_name):
        "Read an annotation for given hemisphere and annotation name."
        # TODO annotation class
        return nibabel.freesurfer.read_annot(self.annot_path(hemi, annot_name))

    def write_annot(self, hemi, annot_name, labels, ctab, names):
        "Write an annotation for given hemisphere from labels, color table and names."
        return nibabel.freesurfer.write_annot(self.annot_path(hemi, annot_name), labels, ctab, names)

    def read_surf(self, hemi, name):
        "Read a hemisphere surface of given type."
        surf_fname = '%s.%s' % (hemi, name)
        surf_path = os.path.join(self.subjects_dir, self.subject, 'surf', surf_fname)
        return nibabel.freesurfer.read_geometry(surf_path)

    def annot_to_lut(self, hemi, annot_name, lut_path):
        "Write out a color look-up table for a given annotation."
        _, ctab, names = self.read_annot(hemi, annot_name)
        with open(lut_path, 'w') as fd:
            for name, (r, g, b, a, id) in zip(names, ctab):
                fd.write('%d\t%s\t%d %d %d %d\n' % (id, name, r, g, b, a))

    # TODO this is more MRtrix than FS
    def annot_to_conn_conf(self, hemi, annot_name, conn_conf_path):
        _, _, names = self.read_annot(hemi, annot_name)
        with open(conn_conf_path, 'w') as fd:
            for id, name in enumerate(names):
                fd.write('%d\t%s\n' % (id, name))

    def compute_gdist_mat(self, surf_name='pial', max_distance=40.0):
        "Compute sparse geodesic distance matrix and save in MATLAB format."
        max_distance = float(max_distance) # in case passed from sys.argv
        for h in 'rl':
            surf_path = '%s/%s/surf/%sh.%s' % (self.subjects_dir, self.subject, h, surf_name)
            v, f = nibabel.freesurfer.read_geometry(surf_path)
            mat_path = '%s/%s/surf/%sh.%s.gdist.mat' % (self.subjects_dir, self.subject, h, surf_name)
            mat = gdist.local_gdist_matrix(v, f.astype('<i4'), max_distance=max_distance)
            scipy.io.savemat(mat_path, {'gdist': mat})

    def convert_bem_to_tri(self):
        surfs_glob = '%s/%s/bem/watershed/*_surface-low' % (self.subjects_dir, self.subject)
        for surf_name in glob.glob(surfs_glob):
            convert_fs_to_brain_visa(surf_name)

