# demo.py

import re
import json

# Sample dataset: a list of documents
DATASET = [
    {"_id": 1, "name": "Alice", "age": 30, "city": "New York"},
    {"_id": 2, "name": "Bob", "age": 25, "city": "San Francisco"},
    {"_id": 3, "name": "Charlie", "age": 35, "city": "Los Angeles"},
    {"_id": 4, "name": "Diana", "age": 28, "city": "New York"},
    {"_id": 5, "name": "Eve", "age": 40, "city": "Chicago"},
]

# Token types
TOKENS = [
    ('DB',         r'db'),
    ('DOT',        r'\.'),
    ('IDENTIFIER', r'[a-zA-Z_][a-zA-Z0-9_]*'),
    ('LPAREN',     r'\('),
    ('RPAREN',     r'\)'),
    ('SKIP',       r'[ \t\n]+'),
    ('FILTER',     r'.+'),  # Capture the rest of the line as the filter
]

# Tokenizer
def tokenize(query):
    token_regex = '|'.join('(?P<%s>%s)' % pair for pair in TOKENS)
    get_token = re.compile(token_regex).match
    pos = 0
    tokens = []
    while pos < len(query):
        mo = get_token(query, pos)
        if mo is None:
            raise SyntaxError(f'Unexpected character {query[pos]!r} at position {pos}')
        kind = mo.lastgroup
        value = mo.group(kind)
        if kind == 'SKIP':
            pass
        else:
            tokens.append((kind, value))
            if kind == 'FILTER':
                break  # The rest is the filter expression
        pos = mo.end()
    return tokens

# AST Nodes
class ASTNode:
    pass

class Query(ASTNode):
    def __init__(self, collection, filter_expr):
        self.collection = collection
        self.filter_expr = filter_expr  # Dictionary parsed from JSON

# Parser
class Parser:
    def __init__(self, tokens):
        self.tokens = tokens
        self.pos = 0

    def current_token(self):
        if self.pos < len(self.tokens):
            return self.tokens[self.pos]
        return ('EOF', '')

    def eat(self, expected_kind):
        token_kind, token_value = self.current_token()
        if token_kind == expected_kind:
            self.pos += 1
            return token_value
        else:
            raise SyntaxError(f'Expected {expected_kind}, got {token_kind}')

    def parse(self):
        return self.parse_query()

    def parse_query(self):
        self.eat('DB')
        self.eat('DOT')
        collection = self.eat('IDENTIFIER')
        self.eat('DOT')
        function = self.eat('IDENTIFIER')
        if function != 'find':
            raise SyntaxError(f"Unsupported function '{function}'")
        self.eat('LPAREN')
        filter_expr = {}
        token_kind, token_value = self.current_token()
        if token_kind == 'FILTER':
            filter_str = token_value.rstrip(')')
            try:
                filter_expr = json.loads(filter_str)
            except json.JSONDecodeError as e:
                raise SyntaxError(f"Invalid JSON filter: {e}")
            self.pos += 1  # Move past the FILTER token
        self.eat('RPAREN')
        return Query(collection, filter_expr)

# Interpreter
class Interpreter:
    def __init__(self, dataset):
        self.dataset = dataset

    def evaluate(self, node):
        if isinstance(node, Query):
            return self.evaluate_query(node)
        else:
            raise ValueError(f'Unknown node type: {type(node)}')

    def evaluate_query(self, node):
        results = []
        for document in self.dataset:
            if self.match_filter(document, node.filter_expr):
                results.append(document)
        return results

    def match_filter(self, document, filter_expr):
        for key, value in filter_expr.items():
            if not self.match_condition(document, key, value):
                return False
        return True

    def match_condition(self, document, key, value):
        if key.startswith('$'):
            if key == '$and':
                return all(self.match_filter(document, cond) for cond in value)
            elif key == '$or':
                return any(self.match_filter(document, cond) for cond in value)
            elif key == '$not':
                return not self.match_filter(document, value)
            else:
                raise ValueError(f"Unsupported operator '{key}'")
        else:
            doc_value = document.get(key)
            if isinstance(value, dict):
                for op, op_value in value.items():
                    if not self.evaluate_operator(doc_value, op, op_value):
                        return False
                return True
            else:
                return doc_value == value

    def evaluate_operator(self, doc_value, operator, value):
        if operator == '$gt':
            return doc_value > value
        elif operator == '$lt':
            return doc_value < value
        elif operator == '$gte':
            return doc_value >= value
        elif operator == '$lte':
            return doc_value <= value
        elif operator == '$eq':
            return doc_value == value
        elif operator == '$ne':
            return doc_value != value
        else:
            raise ValueError(f"Unsupported operator '{operator}'")

# Main function
def main():
    # Example query
    query = 'db.collection.find({"age": {"$gt": 25}, "city": "New York"})'
    tokens = tokenize(query)
    parser = Parser(tokens)
    try:
        ast = parser.parse()
    except SyntaxError as e:
        print(f"Syntax error: {e}")
        return
    interpreter = Interpreter(DATASET)
    try:
        results = interpreter.evaluate(ast)
    except ValueError as e:
        print(f"Runtime error: {e}")
        return
    print("Query Results:")
    for doc in results:
        print(json.dumps(doc, indent=4))

if __name__ == '__main__':
    main()