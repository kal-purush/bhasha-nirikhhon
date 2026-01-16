# import bnf
import bnfparsing
from funcy.strings import re_all

BNF_definition = """
expression: (column path-item+) | ( table path-item+) | path-item+
column: '!' path-item
table: identifier
path-item: index | element
element: '.' identifier
index: '[' integer ']'
identifier: [a-zA-Z0-9]+
integer: [0-9]+
"""

# bnf.grammar(BNF_definition)


class IfStmtParser(bnfparsing.ParserBase):
    @bnfparsing.rule
    def identifier(self, string):
        index = 0
        while index < len(string) and (
            string[index].isalpha() or string[index].isdigit()
        ):
            index = index + 1
        if index == 0:
            return None, string
        return bnfparsing.Token("identifier", string[:index]), string[index:]

    @bnfparsing.rule
    def integer(self, string):
        index = 0
        while index < len(string) and (
            string[index].isalpha() or string[index].isdigit()
        ):
            index = index + 1
        if index == 0:
            return None, string
        return bnfparsing.Token("integer", (string[:index])), string[index:]

    def __init__(self):
        super().__init__(ws_handler=bnfparsing.ignore)
        self.new_rule("expression", "column path-item+ | age-fn path-item+ ")
        # self.new_rule("expression", "column path-item | table path-item | path-item")
        self.new_rule("column", '"$"')
        self.new_rule("age-fn", '"age"')
        self.new_rule("path-item", "index | element")
        self.new_rule("path-item+", "path-item path-item+ | path-item")
        self.new_rule("element", '"." identifier')
        self.new_rule("index", '"[" integer "]"')


def build_path(token):
    if "path-item" in token.tags:
        return build_path(token.children[1])

    if token.token_type == "path-item+":
        return build_path(token.children[0]) + build_path(token.children[1])

    if token.token_type == "identifier":
        return [token.value()]

    if token.token_type == "integer":
        return [int(token.value())]

    print("MISS", token, token.parent)
    return []


def prepare_token(token):
    if token.children[0].value() == "$":
        return "json_path", build_path(token.children[1])
    if token.children[0].value() == "age":
        return "age", build_path(token.children[1])


def duckdb_jsonpath(path, first_run=True):
    if len(path) == 0:
        return ""
    c = path[0]
    rest = path[1:]
    if isinstance(c, int):
        return f"[{c+1}]" + duckdb_jsonpath(rest, False)
    prefix = "."
    if first_run:
        prefix = ""
    return f"{prefix}{c}" + duckdb_jsonpath(rest, False)


def duckdb_age(path):
    return f"datediff('year', {duckdb_jsonpath(path)}::date, current_date )"


DIALECTS = {"duckdb": {"json_path": duckdb_jsonpath, "age": duckdb_age}}


def resolve_string_template(i, dialect):
    exprs = re_all(r"(?P<macro>\(\{(?P<exp>\S*\s+\S+)\s*\}\))", i)
    vs = {}
    for exp in exprs:
        parser = IfStmtParser()
        token = parser.parse(exp["exp"])
        fn, path = prepare_token(token)
        vs[exp["macro"]] = DIALECTS[dialect][fn](path)

    res = i
    for k, v in vs.items():
        res = res.replace(k, v)

    return res


def convert(sql, dialect):
    return resolve_string_template(sql, dialect)