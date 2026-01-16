# -*- coding: utf-8 -*-

import pytest
from bnm.recon.qc.parser.annotation import AnnotationParser
from bnm.tests.base import get_data_file

TEST_SUBJECT = 'freesurfer_fsaverage'
TEST_ANNOT_FOLDER = 'label'


def test_parse_annotation():
    parser = AnnotationParser()
    file_path = get_data_file(TEST_SUBJECT, TEST_ANNOT_FOLDER, "lh.aparc.annot")
    annot = parser.parse(file_path)
    assert annot.region_names == ['unknown', 'bankssts', 'caudalanteriorcingulate', 'caudalmiddlefrontal',
                                  'corpuscallosum', 'cuneus', 'entorhinal', 'fusiform', 'inferiorparietal',
                                  'inferiortemporal', 'isthmuscingulate', 'lateraloccipital', 'lateralorbitofrontal',
                                  'lingual', 'medialorbitofrontal', 'middletemporal', 'parahippocampal', 'paracentral',
                                  'parsopercularis', 'parsorbitalis', 'parstriangularis', 'pericalcarine',
                                  'postcentral', 'posteriorcingulate', 'precentral', 'precuneus',
                                  'rostralanteriorcingulate', 'rostralmiddlefrontal', 'superiorfrontal',
                                  'superiorparietal', 'superiortemporal', 'supramarginal', 'frontalpole',
                                  'temporalpole', 'transversetemporal', 'insula']


def test_parse_not_existent_annotation():
    parser = AnnotationParser()
    file_path = "not_existent_annotation.annot"
    with pytest.raises(IOError):
        parser.parse(file_path)


def test_parse_not_annotation():
    parser = AnnotationParser()
    file_path = get_data_file(TEST_SUBJECT, "mri", "T1.nii.gz")
    with pytest.raises(ValueError):
        parser.parse(file_path)