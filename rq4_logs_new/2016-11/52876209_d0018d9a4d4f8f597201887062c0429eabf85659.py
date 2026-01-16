# -*- coding: utf-8 -*-

import pytest
from bnm.recon.qc.parser.volume import VolumeParser
from bnm.tests.base import get_data_file
from nibabel.filebasedimages import ImageFileError
from nibabel.py3k import FileNotFoundError

TEST_SUBJECT = 'freesurfer_fsaverage'
TEST_VOLUME_FOLDER = 'mri'


def test_parse_volume():
    parser = VolumeParser()
    file_path = get_data_file(TEST_SUBJECT, TEST_VOLUME_FOLDER, "T1.nii.gz")
    volume = parser.parse(file_path)
    assert volume.dimensions == (256, 256, 256)


def test_parse_not_existing_volume():
    parser = VolumeParser()
    file_path = "not-existent-volume.nii.gz"
    with pytest.raises(FileNotFoundError):
        parser.parse(file_path)


def test_parse_not_volume():
    parser = VolumeParser()
    file_path = get_data_file(TEST_SUBJECT, "surf", "lh.pial")
    with pytest.raises(ImageFileError):
        parser.parse(file_path)