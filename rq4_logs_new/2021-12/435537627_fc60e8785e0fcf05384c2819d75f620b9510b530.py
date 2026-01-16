import datetime
from urllib.parse import urlparse

from transparenciagovbr import settings
from transparenciagovbr.spiders.base import TransparenciaBaseSpider


class LicitacaoMixin:
    def make_filename(self, url):
        return settings.DOWNLOAD_PATH / "licitacao" / urlparse(url).path.rsplit("/", maxsplit=1)[-1]


class LicitacoesSpider(LicitacaoMixin, TransparenciaBaseSpider):
    name = "licitacoes"
    base_url = "http://transparencia.gov.br/download-de-dados/licitacoes/{year}{month:02d}"
    publish_frequency = "monthly"
    filename_suffix = "_Licitaç╞o.csv"
    schema_filename = "licitacoes.csv"

    def __init__(self, start_date=None, end_date=None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.start_date = datetime.date.fromisoformat(start_date)
        self.end_date = datetime.date.fromisoformat(end_date)

class EmpenhosRelacionadosSpider(LicitacaoMixin, TransparenciaBaseSpider):
    name = "empenhos_relacionados"
    base_url = "http://transparencia.gov.br/download-de-dados/licitacoes/{year}{month:02d}"
    publish_frequency = "monthly"
    filename_suffix = "_EmpenhosRelacionados.csv"
    schema_filename = "empenhos_relacionados.csv"

    def __init__(self, start_date=None, end_date=None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.start_date = datetime.date.fromisoformat(start_date)
        self.end_date = datetime.date.fromisoformat(end_date)