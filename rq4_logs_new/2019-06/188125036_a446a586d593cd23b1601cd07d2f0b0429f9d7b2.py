import json
import unittest

from .. import Analysis

TEXTO_ANALYSIS = """[
    {
        "name": "Cantidad de individuos por transecto con captura y recaptura",
        "description": "Cantidad de individuos por transecto con captura y recaptura en Isla Cedros",
        "docker_parent_image": "islasgeci/extension:d25d",
        "report": "",
        "results": [
        ],
        "scripts": [
        ],
        "data": [
            {
                "source": "datapackage",
                "path": "roedores_capturarecaptura_cedros",
                "filename": "roedores_capturarecaptura_cedros.csv",
                "version": "d2ca5a04850b",
                "type": "datapackage"
            }
        ],
        "requirements": []
    }
]"""


class TestAnalysis(unittest.TestCase):

    def setUp(self):
        diccionario_analysis = json.loads(TEXTO_ANALYSIS)
        analisis = diccionario_analysis[0]
        self.analisis = Analysis(**analisis)

    def test_datafile_name(self):
        self.assertEqual(
            self.analisis._data[0].filename, "roedores_capturarecaptura_cedros.csv")

    def test_analysis_name(self):
        self.assertEqual(
            self.analisis.name, "Cantidad de individuos por transecto con captura y recaptura")

    def test_is_dependent_on_datafile(self):
        self.assertTrue(self.analisis.is_dependent_on_datafile(
            "roedores_capturarecaptura_cedros.csv"))

    def test_get_url_to_datafile(self):
        self.assertTrue(self.analisis.get_url_to_datafile("roedores_capturarecaptura_cedros.csv"),
                        "https://bitbucket.org/IslasGECI/datapackage/raw/d2ca5a04850b/roedores_capturarecaptura_cedros/roedores_capturarecaptura_cedros.csv")


if __name__ == '__main__':
    unittest.main()