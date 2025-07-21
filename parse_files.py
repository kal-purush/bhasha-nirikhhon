import os
from typing import Dict, Any

import tree_sitter_python as tspython
import tree_sitter_javascript as tsjavascript
import tree_sitter_typescript as tstype
import tree_sitter_c_sharp as tcsharp
import tree_sitter_java as tjava
from tree_sitter import Language, Parser
# from multiprocessing import Pool, cpu_count
# import functools

# # Initialize parsers
PY_LANGUAGE = Language(tspython.language())
JS_LANGUAGE = Language(tsjavascript.language())
TS_LANGUAGE = Language(tstype.language_typescript())
JAVA_LANGUAGE = Language(tjava.language())
C_SHARP_LANGUAGE = Language(tcsharp.language())

parser = Parser(PY_LANGUAGE)
parser_js = Parser(JS_LANGUAGE)
parser_ts = Parser(TS_LANGUAGE)
parser_java = Parser(JAVA_LANGUAGE)
parser_csharp = Parser(C_SHARP_LANGUAGE)

def get_parser(ext):
    parser_map = {
        ".py": parser,
        ".js": parser_js,
        ".ts": parser_ts,
        ".java": parser_java,
        ".cs": parser_csharp
    }
    return parser_map.get(ext)
class FileElementParser:
    def __init__(self):
        
        self.builtin_identifiers = {
        # Python built-ins and keywords
        'int', 'float', 'str', 'bool', 'list', 'dict', 'set', 'tuple', 'object',
        'Exception', 'BaseException', 'type', 'complex', 'bytes', 'bytearray',
        'memoryview', 'range', 'enumerate', 'zip', 'map', 'filter', 'slice',
        'frozenset', 'super', 'NotImplemented', 'Ellipsis', '__name__', '__file__',
        'self', '__init__', 'print',
        'and', 'as', 'assert', 'break', 'class', 'continue', 'def', 'del', 'elif',
        'else', 'except', 'False', 'finally', 'for', 'from', 'global', 'if',
        'import', 'in', 'is', 'lambda', 'None', 'nonlocal', 'not', 'or', 'pass',
        'raise', 'return', 'True', 'try', 'while', 'with', 'yield',

        # Java built-ins and keywords
        'String', 'Object', 'Integer', 'Double', 'Float', 'Boolean', 'Long', 'Short',
        'Byte', 'Character', 'List', 'Map', 'Set', 'ArrayList', 'HashMap', 'HashSet',
        'LinkedList', 'Queue', 'Deque', 'Vector', 'Exception', 'RuntimeException',
        'IOException', 'Thread', 'System', 'Throwable', 'Cloneable', 'Iterable',
        'Comparable', 'AutoCloseable', 'println', 'out',
        'abstract', 'assert', 'boolean', 'break', 'byte', 'case', 'catch', 'char',
        'class', 'const', 'continue', 'default', 'do', 'double', 'else', 'enum',
        'extends', 'final', 'finally', 'float', 'for', 'goto', 'if', 'implements',
        'import', 'instanceof', 'int', 'interface', 'long', 'native', 'new',
        'package', 'private', 'protected', 'public', 'return', 'short', 'static',
        'strictfp', 'super', 'switch', 'synchronized', 'this', 'throw', 'throws',
        'transient', 'try', 'void', 'volatile', 'while', 'true', 'false', 'null',

        # JavaScript/TypeScript built-ins and keywords
        'String', 'Number', 'Boolean', 'Object', 'Array', 'Map', 'Set', 'WeakMap',
        'WeakSet', 'Promise', 'Function', 'Symbol', 'BigInt', 'Error', 'TypeError',
        'Date', 'RegExp', 'console', 'window', 'document', 'Intl', 'Math', 'Reflect',
        'JSON', 'constructor', 'string',
        'break', 'case', 'catch', 'class', 'const', 'continue', 'debugger', 'default',
        'delete', 'do', 'else', 'enum', 'export', 'extends', 'false', 'finally',
        'for', 'function', 'if', 'import', 'in', 'instanceof', 'new', 'null', 'return',
        'super', 'switch', 'this', 'throw', 'true', 'try', 'typeof', 'var', 'void',
        'while', 'with', 'yield',

        # TypeScript-specific
        'any', 'void', 'never', 'unknown', 'undefined', 'null', 'ReadonlyArray',
        'Record', 'Partial', 'Pick', 'Omit', 'keyof', 'infer', 'readonly', 'declare',
        'namespace', 'module', 'type', 'interface', 'implements', 'abstract',

        # C# built-ins and keywords
        'string', 'int', 'float', 'double', 'decimal', 'bool', 'char', 'object',
        'List', 'Dictionary', 'HashSet', 'Array', 'Queue', 'Stack', 'Exception',
        'System', 'DateTime', 'Task', 'Console', 'Enumerable', 'IEnumerable',
        'IDisposable', 'Nullable', 'Span', 'ReadOnlySpan',
        'abstract', 'as', 'base', 'bool', 'break', 'byte', 'case', 'catch', 'char',
        'checked', 'class', 'const', 'continue', 'decimal', 'default', 'delegate',
        'do', 'double', 'else', 'enum', 'event', 'explicit', 'extern', 'false',
        'finally', 'fixed', 'float', 'for', 'foreach', 'goto', 'if', 'implicit',
        'in', 'int', 'interface', 'internal', 'is', 'lock', 'long', 'namespace',
        'new', 'null', 'object', 'operator', 'out', 'override', 'params', 'private',
        'protected', 'public', 'readonly', 'ref', 'return', 'sbyte', 'sealed', 'short',
        'sizeof', 'stackalloc', 'static', 'string', 'struct', 'switch', 'this',
        'throw', 'true', 'try', 'typeof', 'uint', 'ulong', 'unchecked', 'unsafe',
        'ushort', 'using', 'virtual', 'void', 'volatile', 'while'
    }



        self.elements = {
            'identifiers': [],
            'literals': [],
            'variables': [],
            'comments': [],
            'docstrings': [],
            'functions': [],
            'classes': []
        }

    def _is_docstring(self, node) -> bool:
        if node.type != 'expression_statement':
            return False
        parent = node.parent
        if not parent:
            return False
        if parent.type == 'module':
            for child in parent.children:
                if child.type not in ('comment', 'line_comment'):
                    return child == node
        elif parent.type == 'block':
            grand_parent = parent.parent
            if not grand_parent or grand_parent.type not in ('class_definition', 'function_definition'):
                return False
            for child in parent.children:
                if child.type not in ('comment', 'line_comment'):
                    return child == node
        return False

    def is_builtin_identifier(self, name: str) -> bool:
        return name in self.builtin_identifiers

    def collect_user_defined_classes(self, node, class_names):
        if node.type in ('class_definition', 'class_declaration'):
            for child in node.children:
                if child.type in ('identifier', 'type_identifier'):
                    class_name = child.text.decode('utf8')
                    node_id = child.id
                    if not self.is_builtin_identifier(class_name):
                        if (class_name, node_id) not in class_names:
                            class_names.append((class_name, node_id))
        for child in node.children:
            if child:
                self.collect_user_defined_classes(child, class_names)

    
    def collect_user_defined_functions(self, node, function_names):
        if node.type in (
            'function_definition',
            'function_declaration',
            'method_definition',
            'method_declaration',
            'constructor_declaration'
        ):
            for child in node.children:
                if child.type in ('identifier', 'type_identifier', 'property_identifier'):
                    func_name = child.text.decode('utf8')
                    node_id = child.id
                    if not self.is_builtin_identifier(func_name) and func_name != '__init__':
                        if (func_name, node_id) not in function_names:
                            function_names.append((func_name, node_id))

        for child in node.children:
            if child:
                self.collect_user_defined_functions(child, function_names)


    def is_variable(self, node, class_names=None, function_names=None, ext=None) -> bool:
        if ext=='.java':
            return False
        if node.type != 'identifier':
            return False

        name = node.text.decode('utf8')
        if self.is_builtin_identifier(name):
            return False
        
        if name =='__main__':
            return False

        parent = node.parent
        grandparent = parent.parent if parent else None

        # Skip identifiers in class/function definitions
        if parent and parent.type in (
            'class_definition', 'function_definition',
            'method_definition', 'function_declaration',
            'method_declaration', 'constructor_declaration'
        ):
            return False

        # Skip function calls
        if parent and parent.type == 'call':
            return False

        # Skip import statements
        if parent and parent.type in ('import_statement', 'import_from_statement'):
            return False

        # Left-hand side of assignment (e.g., a = ...)
        if parent and parent.type == 'assignment':
            return parent.children[0] == node

        # Variable declarators (e.g., int x = 1;) in Java, C#, JS, TS
        if parent and parent.type in ('variable_declarator', 'lexical_declarator'):
            return True

        # Destructuring patterns (e.g., const {x} = obj or [a, b] = list)
        if parent and parent.type in ('object_pattern', 'array_pattern', 'pair_pattern'):
            return True

        # Python-style annotated assignment: x: int = 5
        if grandparent and grandparent.type == 'assignment':
            return grandparent.children[0] == parent

        # Fallback: if identifier appears under a variable_declaration block
        if parent and parent.type in ('variable_declaration', 'field_declaration', 'property_declaration'):
            return True

        return False


    def extract_elements(self, node, source_code: bytes, class_names=None,  function_names=None, ext=None) -> None:
        node_type = node.type
        # print("Node Type: ", node_type)
        node_text = node.text.decode('utf8')
        node_id = node.id
        # print("Node Type: ", node_type, node_text, node.id)

        if node_type == 'identifier':
            name = node_text
            if not self.is_builtin_identifier(name) and (name, node_id) not in function_names and (name, node_id) not in class_names:
                if self.is_variable(node, class_names, function_names, ext):
                    if (name, node_id) not in self.elements['variables']:
                        self.elements['variables'].append((name, node_id))
                else:
                    if name != '__main__':
                        if (name, node_id) not in self.elements['identifiers']:
                            self.elements['identifiers'].append((name, node_id))

        elif node_type in ('string_literal', 'string'):
            text = node_text.strip("")
            if ext==".py" and (node_text.startswith("\"\"\"") or node_text.startswith("'''")):
                if (text, node_id) not in self.elements['docstrings']:
                    self.elements['docstrings'].append((text, node_id))
            else:
                if (text, node_id) not in self.elements['literals']:
                    self.elements['literals'].append((text, node_id))
        
        elif node_type in ('template_string', 'template_literal') and ext in ('.js', '.ts'):
            text = node_text.strip('`').strip()
            if (text, node_id) not in self.elements['literals']:
                self.elements['literals'].append((text, node_id))

        elif node_type == "variable_declarator" and ext == '.java':
            name_child = node.child_by_field_name("name")
            value_child = node.child_by_field_name("value")

            # Ensure it's just an identifier and there's no value/assignment
            if name_child is not None and name_child.type == "identifier" and value_child is None:
                node_text = node.text.decode('utf8')  # assuming you have this method
                # print("SIMPLE VAR ====>", node_text, "NODE:", node, node_type)
                if (node_text, node_id) not in self.elements['variables']:
                    self.elements['variables'].append((node_text, node_id))
        
        elif node_type =="block_comment":
            text = node_text[3:-2].strip()  # Java/JSDoc-style
            if (text, node_id) not in self.elements['docstrings']:
                self.elements['docstrings'].append((text, node_id))
        
        elif node_type in ('comment', 'line_comment'):
            text = node_text.strip()
            # print("comment ======> ",text)
            # Handle common comment styles
            if text.startswith('#'):
                text = text.lstrip('#').strip()
            elif text.startswith('///'):
                text = text.lstrip('/').strip()  # C# XML-style
            elif text.startswith('//'):
                text = text.lstrip('/').strip()  # JS/TS/Java/C#
            elif text.startswith('/**') and text.endswith('*/'):
                text = text[3:-2].strip()  # Java/JSDoc-style
            elif text.startswith('/*') and text.endswith('*/'):
                text = text[2:-2].strip()

            if text.startswith('*') or text.startswith('<summary'):
                if (text, node_id) not in self.elements['docstrings']:
                    self.elements['docstrings'].append((text, node_id))
            else:
                if (text, node_id) not in self.elements['comments']:
                    self.elements['comments'].append((text, node_id))
        
        elif node_type == "arrow_function" and ext in ('.js', '.ts'):
            # Get function name from parent declarator if available
            parent = node.parent
            if parent and parent.type in ('variable_declarator', 'lexical_declarator'):
                for child in parent.children:
                    if child.type == 'identifier':
                        func_name = child.text.decode('utf8')
                        if not self.is_builtin_identifier(func_name):
                            if (func_name, node_id) not in self.elements['identifiers']:
                                self.elements['identifiers'].append((func_name, node_id))
                            if (func_name, node_id) not in self.elements['functions']:
                                self.elements['functions'].append((func_name, node_id))
        for child in node.children:
            self.extract_elements(child, source_code, class_names, function_names, ext)
        
    def parse_file(self, file_path: str, ext: str) -> Dict[str, Any]:
        try:
            parser = get_parser(ext)
            # print(ext)
            with open(file_path, 'rb') as f:
                source_code = f.read()

            tree = parser.parse(source_code)
            self.elements = {k: [] for k, v in self.elements.items()}
            class_names = []
            self.collect_user_defined_classes(tree.root_node, class_names)
            function_names = []
            self.collect_user_defined_functions(tree.root_node, function_names)
            # print(file_path, class_names, function_names)
            self.extract_elements(tree.root_node, source_code, class_names, function_names, ext)
            result = {
                'identifiers': [name for name, _ in self.elements['identifiers']],
                'literals': [text for text, _ in self.elements['literals']],
                'variables': [name for name, _ in self.elements['variables']],
                'comments': [text for text, _ in self.elements['comments']],
                'docstrings': [text for text, _ in self.elements['docstrings']],
                'functions': [name for name, _ in function_names],
                'classes': [name for name, _ in class_names]
            }

            return result

        except Exception as e:
            return None

import glob
from pathlib import Path
import json
from tqdm import tqdm
supported_extensions = {'.py', '.java', '.js', '.ts', '.cs'}
# def main():
#     parser = FileElementParser()
#     log_dir = "logs"
#     folders = glob.glob(f"{log_dir}/*")
#     count = 0

#     for folder in folders:
#         files = glob.glob(f"{folder}/*")
#         for file_ in tqdm(files, desc=f"Parsing files in {folder}"):
#             ext = Path(file_).suffix.lower()
#             if ext in supported_extensions:
#             # Extract year and month from folder name (assuming format: logs/YYYY-MM or similar)
#                 folder_name = os.path.basename(folder)
#                 parts = folder_name.split('-')
#                 if len(parts) >= 2:
#                     year, month = parts[0], parts[1]
#                 else:
#                     year, month = "unknown", "unknown"

#                 result = parser.parse_file(file_, ext)
#                 if result:
#                     out_dir = f"code_paerser_data/{year}-{month}"
#                     os.makedirs(out_dir, exist_ok=True)
#                     base_name = os.path.basename(file_)
#                     json_name = f"{os.path.splitext(base_name)[0]}_{year}_{month}.json"
#                     out_path = os.path.join(out_dir, json_name)
#                     with open(out_path, "w", encoding="utf-8") as jf:
#                         json.dump(result, jf, ensure_ascii=False, indent=4)
                
#     print("\nTotal files parsed:", count)

import glob
from pathlib import Path
import json
from tqdm import tqdm
from concurrent.futures import ProcessPoolExecutor, as_completed

supported_extensions = {'.py', '.java', '.js', '.ts', '.cs'}

# def process_file(args):
#     file_, folder = args
#     ext = Path(file_).suffix.lower()
#     if ext not in supported_extensions:
#         return None
#     # Extract year and month from folder name (assuming format: logs/YYYY-MM or similar)
#     folder_name = os.path.basename(folder)
#     parts = folder_name.split('-')
#     if len(parts) >= 2:
#         year, month = parts[0], parts[1]
#     else:
#         year, month = "unknown", "unknown"
#     parser = FileElementParser()
#     result = parser.parse_file(file_, ext)
#     if result:
#         out_dir = f"code_paerser_data_rq4/{year}-{month}"
#         os.makedirs(out_dir, exist_ok=True)
#         base_name = os.path.basename(file_)
#         json_name = f"{os.path.splitext(base_name)[0]}_{year}_{month}.json"
#         out_path = os.path.join(out_dir, json_name)
#         with open(out_path, "w", encoding="utf-8") as jf:
#             json.dump(result, jf, ensure_ascii=False, indent=4)
#         return 1
#     return 0

def process_file(args):
    file_, folder = args
    ext = Path(file_).suffix.lower()
    if ext not in supported_extensions:
        return None

    folder_name = os.path.basename(folder)
    parts = folder_name.split('-')
    if len(parts) >= 2:
        year, month = parts[0], parts[1]
    else:
        year, month = "unknown", "unknown"

    base_name = os.path.basename(file_)
    json_name = f"{os.path.splitext(base_name)[0]}_{year}_{month}.json"
    out_dir = f"code_paerser_data_rq4/{year}-{month}"
    out_path = os.path.join(out_dir, json_name)

    if os.path.exists(out_path):
        return 0  # Skip if already processed

    parser = FileElementParser()
    result = parser.parse_file(file_, ext)
    if result:
        os.makedirs(out_dir, exist_ok=True)
        with open(out_path, "w", encoding="utf-8") as jf:
            json.dump(result, jf, ensure_ascii=False, indent=4)
        return 1
    return 0


def main():
    log_dir = "rq4_logs_new"
    folders = glob.glob(f"{log_dir}/*")
    file_folder_pairs = []
    for folder in folders:
        files = glob.glob(f"{folder}/*")
        for file_ in files:
            file_folder_pairs.append((file_, folder))

    count = 0
    num_workers = os.cpu_count()
    with ProcessPoolExecutor(max_workers=num_workers) as executor:
        futures = [executor.submit(process_file, args) for args in file_folder_pairs]
        for f in tqdm(as_completed(futures), total=len(futures), desc="Processing files in parallel"):
            try:
                count += f.result()
            except Exception:
                pass

    print("\nTotal files parsed:", count)


if __name__ == "__main__":
    main()
