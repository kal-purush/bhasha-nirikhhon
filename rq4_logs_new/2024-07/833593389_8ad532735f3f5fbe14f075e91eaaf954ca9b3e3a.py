from pandera import Check, Column, Index, SchemaModel


class SchemaCRM(SchemaModel):
    id_produto: Column[int] = Column(
        dtype="int64",
        checks=[
            Check.greater_than_or_equal_to(min_value=1.0),
            Check.less_than_or_equal_to(max_value=10.0),
        ],
        nullable=False,
        unique=False,
        coerce=False,
        required=True,
        regex=False,
        description=None,
        title=None,
    )
    nome: Column[str] = Column(
        dtype="object",
        checks=None,
        nullable=False,
        unique=False,
        coerce=False,
        required=True,
        regex=False,
        description=None,
        title=None,
    )
    quantidade: Column[int] = Column(
        dtype="int64",
        checks=[
            Check.greater_than_or_equal_to(min_value=20.0),
            Check.less_than_or_equal_to(max_value=200.0),
        ],
        nullable=False,
        unique=False,
        coerce=False,
        required=True,
        regex=False,
        description=None,
        title=None,
    )
    preco: Column[float] = Column(
        dtype="float64",
        checks=[
            Check.greater_than_or_equal_to(min_value=5.0),
            Check.less_than_or_equal_to(max_value=120.0),
        ],
        nullable=False,
        unique=False,
        coerce=False,
        required=True,
        regex=False,
        description=None,
        title=None,
    )
    categoria: Column[str] = Column(
        dtype="object",
        checks=None,
        nullable=False,
        unique=False,
        coerce=False,
        required=True,
        regex=False,
        description=None,
        title=None,
    )

    index: Index[int] = Index(
        dtype="int64",
        checks=[
            Check.greater_than_or_equal_to(min_value=0.0),
            Check.less_than_or_equal_to(max_value=9.0),
        ],
        nullable=False,
        coerce=False,
        name=None,
        description=None,
        title=None,
    )

    class Config:
        coerce = True
        strict = False
        unique = None
        report_duplicates = "all"
        unique_column_names = False
        add_missing_columns = False
        title = None
        description = None