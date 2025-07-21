import csv
import glob
import os
import re
import ast
import pandas as pd
from pathlib import Path
from collections import defaultdict
from multiprocessing import Pool, cpu_count
from tqdm import tqdm
import tokenize
import io
import keyword
import builtins
from tree_sitter import Language, Parser

import tree_sitter_python as tspython
import tree_sitter_javascript as tsjavascript
import tree_sitter_typescript as tstype
# from multiprocessing import Pool, cpu_count
# import functools

# # Initialize parsers
PY_LANGUAGE = Language(tspython.language())
JS_LANGUAGE = Language(tsjavascript.language())
TS_LANGUAGE = Language(tstype.language_typescript())
parser = Parser(PY_LANGUAGE)
parser_js = Parser(JS_LANGUAGE)
parser_ts = Parser(TS_LANGUAGE)


def extract_entities_tree_sitter(code, parser):
    tree = parser.parse(bytes(code, "utf8"))
    root_node = tree.root_node

    result = defaultdict(list)

    def get_text(node):
        return code[node.start_byte:node.end_byte]

    def walk(node):
        if node.type == "function_definition":
            print("Function ", node)
            name = get_text(node)
            if name:
                result["functions"].append(name.strip())
        elif node.type == "class_declaration":
            name = get_text(node)
            if name:
                result["classes"].append(name.strip())
        elif node.type == "string":
            name = get_text(node)
            if name:
                result["literals"].append(name.strip('"\''))
        elif node.type == "comment":
            name = get_text(node)
            if name:
                result["comments"].append(name.strip())
        elif node.type == "identifier":
            name = get_text(node)
            if name:
                result["identifier"].append(name.strip())
        
        elif node.type == "regex":
            result["regex"].append(get_text(node))
        for c in node.children:
            walk(c)

    walk(root_node)

    inline_regexes = re.findall(r'(?<!\w)/((?:\\.|[^/\n])+)/[gimsuy]*', code)
    result["regex"].extend(inline_regexes)
    return result

def extract_entities_python(code):
    results = defaultdict(list)

    # Always extract comments and docstrings first using tokenize
    try:
        tokens = list(tokenize.generate_tokens(io.StringIO(code).readline))
        for idx, (tok_type, tok_str, _, _, _) in enumerate(tokens):
            if tok_type == tokenize.COMMENT:
                results["comments"].append(tok_str.lstrip("#").strip())
    except Exception as e:
        pass
        # print("Error tokenizing code:", e)

    # Try parsing with AST
    try:
        tree = ast.parse(code)
    except Exception:
        tree = None

    if tree:
        for node in ast.walk(tree):
            if isinstance(node, ast.FunctionDef):
                results["functions"].append(node.name)
            elif isinstance(node, ast.ClassDef):
                results["classes"].append(node.name)
            elif isinstance(node, ast.Name):
                results["identifiers"].append(node.id)
            elif isinstance(node, ast.Str):
                results["literals"].append(node.s)
            elif isinstance(node, ast.Constant):  # Python 3.8+
                if isinstance(node.value, str):
                    results["literals"].append(node.value)
    else:
        # Fallback: extract functions, classes, identifiers, literals via regex

        for tok_type, tok_str, _, _, _ in tokens:  
            if tok_type == tokenize.NAME and tok_str not in keyword.kwlist and tok_str not in builtins.__dict__:
                results["identifiers"].append(tok_str)
            elif tok_type == tokenize.STRING:
                results["literals"].append(tok_str.strip())

        func_matches = re.findall(r'(?:async\s+)?def\s+([A-Za-z_][A-Za-z0-9_]*)\s*\(', code)
        results["functions"].extend(func_matches)

        class_matches = re.findall(r'class\s+([A-Za-z_][A-Za-z0-9_]*)\s*(?:\(|:)', code)
        results["classes"].extend(class_matches)

    # Extract regex patterns from original code
    matches = re.finditer(r'\br([\'"])(.*?)(?<!\\)\1|re\.compile\((.*?)(?<!\\)\)', code)
    regex_patterns = [m.group(0) for m in matches]
    results["regex"].extend(regex_patterns)

    # for key in results:
    #     print(f"{key}: {results[key]}")

    return results


def process_commit_data(args):
    repo_id, commit = args
    lines = commit.splitlines()
    if not lines or not lines[0].startswith("commit") or "[bot]" in lines[1]:
        return []

    chash = lines[0].split()[1]
    date = lines[2] if len(lines) > 2 else ""

    message_lines = []
    for line in lines[3:]:
        if line.startswith("diff --git"):
            break
        message_lines.append(line.strip())

    message = " ".join(m for m in message_lines if m)
    entries = []
    diffs = re.split(r'diff --git a/', commit)

    for diff in diffs[1:]:
        lines = diff.splitlines()
        if not lines:
            continue
        fpath = lines[0].split()[0]
        ext = Path(fpath).suffix.lower()
        if ext not in target_exts:
            continue

        is_new = is_new_file(lines)
        patch_lines = [line[1:] for line in lines[1:] if line.startswith("+") and not line.startswith("+++")]

        if not patch_lines:
            continue

        if is_new:
            patch = "\n".join(patch_lines)
            ents = extract_entities(patch, ext)
        else:
            patch = "\n".join(patch_lines)
            ents = parse_patch_by_line(patch, ext)

        if ext == ".py":
            entries.append({
                "repo_id": repo_id,
                "date": date,
                "commit": chash,
                "message": message,
                "file": fpath,
                "ext": ext,
                "functions": ", ".join(ents.get("functions", [])),
                "classes": ", ".join(ents.get("classes", [])),
                "identifiers": ", ".join(ents.get("identifiers", [])),
                "literals": ", ".join(ents.get("literals", [])),
                "comments": ", ".join(ents.get("comments", [])),
                "docstrings": ", ".join(ents.get("docstrings", [])),
                "regex": ", ".join(ents.get("regex", [])),
            })
        elif ext == ".md":
            entries.append({
                "repo_id": repo_id,
                "date": date,
                "commit": chash,
                "message": message,
                "file": fpath,
                "ext": ext,
                "headings": ", ".join(f"{level}:{text}" for level, text in ents.get("headings", [])),
                "code_blocks": ", ".join(f"{lang}:{block}" for lang, block in ents.get("code_blocks", [])),
                "inline_code": ", ".join(ents.get("inline_code", [])),
                "links": ", ".join(f"{text}:{url}" for text, url in ents.get("links", [])),
                "text": ", ".join(ents.get("text", [])),
            })
    return entries

from multiprocessing import TimeoutError  # add this at the top if not already

# def process_large_log_file(fname):
#     file_path = os.path.join("logs", fname)
#     size_bytes = os.path.getsize(file_path)
#     if size_bytes > 1000000000:
#         print(f"Skipping {fname} (size > 1GB)")
#         return -1

#     print(f"Processing file: {fname}")
#     repo_id = Path(fname).stem
#     inputs = [(repo_id, commit) for commit in read_commits_stream(file_path)]
#     # print("number of commit", len(inputs))
#     results = []
#     count = 0
#     with Pool(cpu_count()) as pool:
#         it = pool.imap_unordered(process_commit_data, inputs)
#         for _ in tqdm(range(len(inputs)), desc=f"Processing {fname}"):
#             try:
#                 res = it.next(timeout=60)  # 15 minutes timeout per task
#                 results.append(res)
#             except TimeoutError:
#                 results.append([])
#             except Exception:
#                 results.append([])

#     flat_results = [entry for sublist in results for entry in sublist]
#     if flat_results:
#         os.makedirs("parsed_json", exist_ok=True)
#         out_file = os.path.join("parsed_json", f"{repo_id}_parsed.json")
#         with open(out_file, "w", encoding="utf-8") as f:
#             import json
#             json.dump(flat_results, f, ensure_ascii=False, indent=2)
#         print(f"Saved results to {out_file}")
#     else:
#         print(f"No valid entries found in {fname}")

if __name__ == "__main__":
    log_dir = "logs"
    folders = glob.glob(f"{log_dir}/*")
    for folder in folders:
        files = glob.glob(folder+"/*")
        count = 0
        for file_ in files:
            fpath = file_.split()[0]
            ext = Path(fpath).suffix.lower()
            if ext==".py":
                with open(file_, "r", encoding="utf-8") as f:
                    content = f.read()
                    ents = extract_entities_tree_sitter(content, parser)
                    for key in ents:
                        print(key, "====> ", ents[key])
                    count+=1
                    break
        print("Total files:", count)
       