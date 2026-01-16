from enum import Enum

TemplateFileFormat = Enum(
    value="TemplateFileFormat",
    names="YAML JSON TOML",
    module=__name__,
    qualname="letra.TemplateFileFormat",
)