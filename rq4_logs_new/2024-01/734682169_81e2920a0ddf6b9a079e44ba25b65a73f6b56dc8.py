"""Pytest fixtures for the tests in the tests/ directory"""

# pylint: disable=line-too-long

import shutil
import requests
import os
import pytest


def pytest_sessionstart():
    """Download assemblies from NCBI"""
    assemblies = {
        "GCF_000006945.2_ASM694v2_genomic.fna": "https://api.ncbi.nlm.nih.gov/datasets/v2alpha/genome/accession/GCF_000006945.2/download?include_annotation_type=GENOME_FASTA",
        "GCF_000018445.1_ASM1844v1_genomic.fna": "https://api.ncbi.nlm.nih.gov/datasets/v2alpha/genome/accession/GCF_000018445.1/download?include_annotation_type=GENOME_FASTA",
        "GCF_000069245.1_ASM6924v1_genomic.fna": "https://api.ncbi.nlm.nih.gov/datasets/v2alpha/genome/accession/GCF_000069245.1/download?include_annotation_type=GENOME_FASTA",
    }
    if not os.path.exists("tests/test_assemblies"):
        os.makedirs("tests/test_assemblies")

    for assembly, url in assemblies.items():
        if not os.path.exists("tests/test_assemblies/" + assembly):
            print("Downloading " + assembly)

            r = requests.get(url, allow_redirects=True, timeout=10)
            open("tests/test_assemblies/" + assembly + ".zip", "wb").write(r.content)

            # Unzip and move
            shutil.unpack_archive(
                "tests/test_assemblies/" + assembly + ".zip",
                "tests/test_assemblies/temp",
                "zip",
            )
            refseq_id = "_".join(assembly.split("_", 2)[:2])
            shutil.move(
                "tests/test_assemblies/temp/ncbi_dataset/data/"
                + refseq_id
                + "/"
                + assembly,
                "tests/test_assemblies/" + assembly,
            )

            # Clean up
            shutil.rmtree("tests/test_assemblies/temp")
            os.remove("tests/test_assemblies/" + assembly + ".zip")


@pytest.fixture
def assembly_dir_path(tmp_path, request):
    """Create a temporary directory with requested test assembly and return the path as string"""
    shutil.copy("tests/test_assemblies/" + request.param, tmp_path)
    return tmp_path.as_posix()


@pytest.fixture
def assembly_file_path(tmp_path, request):
    """Create a temporary directory with requested test assembly and returns the path to the file"""
    file_path = shutil.copy("tests/test_assemblies/" + request.param, tmp_path)
    return file_path