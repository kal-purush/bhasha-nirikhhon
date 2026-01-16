import pandera as pa
from pandera.typing import Series

email_regex = r"[ˆ@]+@[^@]+\.[^@]+"


class ProdutoSchemaEmail(pa.DataFrameModel):
    """
    Um modelo de esquema para validar um DataFrame contendo informações de produtos usando pandera.

    Atributos:
    ----------
    id_produto : Series[int]
        Um campo inteiro que representa o ID do produto. Este campo não é anulável e seus valores são convertidos para inteiros.

    nome : Series[str]
        Um campo de string que representa o nome do produto. Este campo não é anulável e seus valores são convertidos para strings.

    quantidade : Series[float]
        Um campo float que representa a quantidade do produto. Este campo tem uma restrição de intervalo com valores entre
        -150.0 e 500.0 (inclusive). Ele não é anulável e seus valores são convertidos para floats.

    preco : Series[float]
        Um campo flo at que representa o preço do produto. Este campo tem uma restrição de intervalo com valores entre
        2.0 e 2000.0 (inclusive). Ele não é anulável e seus valores são convertidos para floats.

    categoria : Series[str]
        Um campo de string que representa a categoria do produto. Este campo não é anulável e seus valores são convertidos para strings.
    email : Series[str]
        Um campo de string que representa o email do cliente. Este campo não é anulável e seus valores são convertidos para strings.
        Este campo tem uma restrição de regex para validar o formato do email.
    Config:
    -------
    coerce : bool
        Uma configuração para garantir que os tipos de dados sejam convertidos conforme especificado.

    strict : bool
        Uma configuração para garantir a validação estrita do esquema.
    """

    id_produto: Series[int] = pa.Field(nullable=False, coerce=True)
    nome: Series[str] = pa.Field(nullable=False, coerce=True)
    quantidade: Series[float] = pa.Field(
        ge=-150.0, le=500.0, nullable=False, coerce=True
    )
    preco: Series[float] = pa.Field(ge=2.0, le=2000.0, nullable=False, coerce=True)
    categoria: Series[str] = pa.Field(nullable=False, coerce=True)
    email: Series[str] = pa.Field(regex=email_regex, nullable=False, coerce=True)

    class Config:
        coerce = True
        strict = True