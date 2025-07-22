# import os
# import re
# import ast
# import pandas as pd
# from pathlib import Path
# from collections import defaultdict
# from tree_sitter import Language, Parser
# from tqdm import tqdm
# import tree_sitter_python as tspython
# import tree_sitter_javascript as tsjavascript
# import tree_sitter_typescript as tstype
# from multiprocessing import Pool, cpu_count
# import functools

# # Initialize parsers
# PY_LANGUAGE = Language(tspython.language())
# JS_LANGUAGE = Language(tsjavascript.language())
# TS_LANGUAGE = Language(tstype.language_typescript())
# parser = Parser(PY_LANGUAGE)
# parser_js = Parser(JS_LANGUAGE)
# parser_ts = Parser(TS_LANGUAGE)

# target_exts = {".js", ".jsx", ".ts", ".py"}

# def extract_entities_tree_sitter(code, parser):
#     tree = parser.parse(bytes(code, "utf8"))
#     root_node = tree.root_node

#     result = defaultdict(list)

#     def get_text(node):
#         return code[node.start_byte:node.end_byte]

#     def walk(node):
#         if node.type == "string":
#             name = get_text(node)
#             if name:
#                 result["literals"].append(name.strip('"\''))
#         elif node.type == "comment":
#             name = get_text(node)
#             if name:
#                 result["comments"].append(name.strip())
#         elif node.type == "identifier":
#             name = get_text(node)
#             if name:
#                 result["identifier"].append(name.strip())
#         elif node.type == "function_definition":
#             name = get_text(node)
#             if name:
#                 result["functions"].append(name.strip())
#         elif node.type == "class_declaration":
#             name = get_text(node)
#             if name:
#                 result["classes"].append(name.strip())
#         elif node.type == "regex":
#             result["regex"].append(get_text(node))
#         for c in node.children:
#             walk(c)

#     walk(root_node)

#     inline_regexes = re.findall(r'(?<!\w)/((?:\\.|[^/\n])+)/[gimsuy]*', code)
#     result["regex"].extend(inline_regexes)
#     return result

# def extract_entities_python(code):
#     entities = defaultdict(list)
#     try:
#         tree = ast.parse(code)
#         for node in ast.walk(tree):
#             if isinstance(node, ast.FunctionDef):
#                 entities["functions"].append(node.name)
#                 if ast.get_docstring(node):
#                     entities["docstrings"].append(ast.get_docstring(node))
#             elif isinstance(node, ast.ClassDef):
#                 entities["classes"].append(node.name)
#                 if ast.get_docstring(node):
#                     entities["docstrings"].append(ast.get_docstring(node))
#             elif isinstance(node, ast.Name):
#                 entities["variables"].append(node.id)
#             elif isinstance(node, (ast.Str, ast.Constant)):
#                 val = getattr(node, "s", getattr(node, "value", ""))
#                 if isinstance(val, str):
#                     entities["literals"].append(val)
#     except Exception:
#         pass
#     return entities

# def extract_entities(code, ext):
#     if ext in [".js", ".jsx"]:
#         return extract_entities_tree_sitter(code, parser_js)
#     elif ext == ".ts":
#         return extract_entities_tree_sitter(code, parser_ts)
#     else:
#         return extract_entities_tree_sitter(code, parser)

# def process_commit_file(repo_id, commit_info, diff):
#     """Process a single file diff within a commit"""
#     header, *body = diff.splitlines()
#     fpath = header.split()[0]
#     ext = Path(fpath).suffix.lower()
#     if ext not in target_exts:
#         return None

#     patch = "\n".join(line[1:] for line in body if line.startswith(("+")))
    
#     ents = extract_entities(patch, ext)
    
#     return {
#         "repo_id": repo_id,
#         "date": commit_info["date"],
#         "commit": commit_info["commit"],
#         "file": fpath,
#         "ext": ext,
#         "functions": ", ".join(ents.get("functions", [])),
#         "classes": ", ".join(ents.get("classes", [])),
#         "variables": ", ".join(ents.get("variables", [])),
#         "literals": ", ".join(ents.get("literals", [])),
#         "comments": ", ".join(ents.get("comments", [])),
#         "docstrings": ", ".join(ents.get("docstrings", [])),
#         "regex": ", ".join(ents.get("regex", [])),
#     }

# def process_commit(repo_id, commit):
#     """Process a single commit"""
#     lines = commit.splitlines()
#     author_line = lines[1] if len(lines) > 1 else ""
#     if "[bot]" in author_line:
#         return []
    
#     if not lines or not lines[0].startswith("commit"):
#         return []
    
#     chash = lines[0].split()[1]
#     date = lines[2] if len(lines) > 2 else ""
    
#     commit_info = {
#         "commit": chash,
#         "date": date
#     }
    
#     diffs = re.split(r'diff --git a/', commit)
#     entries = []
    
#     for diff in diffs[1:]:
#         result = process_commit_file(repo_id, commit_info, diff)
#         if result:
#             entries.append(result)
    
#     return entries

# def process_log_file(fname):
#     """Process a single log file"""
#     pid = os.getpid()
#     file_path = os.path.join("logs", fname)
#     file_size = os.path.getsize(file_path)
#     print(f"[PID {pid}] Processing file: {fname} (size: {file_size} bytes)")
    
#     if not fname.endswith(".txt"):
#         return []
    
#     repo_id = Path(fname).stem
#     content = open(os.path.join("logs", fname), encoding="utf-8", errors="replace").read()
#     commits = re.split(r'\n(?=commit\s[0-9a-f]{40})', content.strip())
    
#     all_entries = []
#     for commit in commits:
#         all_entries.extend(process_commit(repo_id, commit))
#     print(f"[PID {pid}] Processing file: {fname} done ================================")
#     return all_entries

# def main():
#     log_dir = "logs"
#     log_files = [fname for fname in os.listdir(log_dir) if fname.endswith(".txt")]
    
#     # Use all available CPUs minus one (to keep system responsive)
#     num_processes = max(1, cpu_count())
    
#     print(f"Processing {len(log_files)} log files using {num_processes} processes...")
    
#     with Pool(processes=num_processes) as pool:
#         # Use imap_unordered for faster processing (order doesn't matter)
#         results = list(pool.imap_unordered(process_log_file, log_files))
    
#     # Flatten the list of lists
#     entries = [entry for sublist in results for entry in sublist]
    
#     df = pd.DataFrame(entries)
#     df.to_csv("parsed_commits.csv", index=False)
#     print("Done: parsed_commits.csv")

# if __name__ == "__main__":
#     main()


import os
import re
import ast
import pandas as pd
from pathlib import Path
from collections import defaultdict
from tree_sitter import Language, Parser
import tree_sitter_python as tspython
import tree_sitter_javascript as tsjavascript
import tree_sitter_typescript as tstype
from multiprocessing import Pool, cpu_count
from tqdm import tqdm

# Initialize parsers
PY_LANGUAGE = Language(tspython.language())
JS_LANGUAGE = Language(tsjavascript.language())
TS_LANGUAGE = Language(tstype.language_typescript())
parser = Parser(PY_LANGUAGE)
parser_js = Parser(JS_LANGUAGE)
parser_ts = Parser(TS_LANGUAGE)

target_exts = {".js", ".jsx", ".ts", ".py"}

def extract_entities_tree_sitter(code, parser):
    tree = parser.parse(bytes(code, "utf8"))
    root_node = tree.root_node
    result = defaultdict(list)

    def get_text(node):
        return code[node.start_byte:node.end_byte]

    def walk(node, depth=0, max_depth=500):
        if depth > max_depth:
            return

        if node.type == "string":
            result["literals"].append(get_text(node).strip('"\''))
        elif node.type == "comment":
            result["comments"].append(get_text(node).strip())
        elif node.type == "identifier":
            result["identifier"].append(get_text(node).strip())
        elif node.type == "function_definition":
            result["functions"].append(get_text(node).strip())
        elif node.type == "class_declaration":
            result["classes"].append(get_text(node).strip())
        elif node.type == "regex":
            result["regex"].append(get_text(node))

        for c in node.children:
            walk(c, depth + 1, max_depth)

    walk(root_node, depth=0)
    inline_regexes = re.findall(r'(?<!\w)/((?:\\.|[^/\n])+)/[gimsuy]*', code)
    result["regex"].extend(inline_regexes)
    return result

def extract_entities_python(code):
    entities = defaultdict(list)
    try:
        tree = ast.parse(code)
        for node in ast.walk(tree):
            if isinstance(node, ast.FunctionDef):
                entities["functions"].append(node.name)
                if ast.get_docstring(node):
                    entities["docstrings"].append(ast.get_docstring(node))
            elif isinstance(node, ast.ClassDef):
                entities["classes"].append(node.name)
                if ast.get_docstring(node):
                    entities["docstrings"].append(ast.get_docstring(node))
            elif isinstance(node, ast.Name):
                entities["variables"].append(node.id)
            elif isinstance(node, (ast.Str, ast.Constant)):
                val = getattr(node, "s", getattr(node, "value", ""))
                if isinstance(val, str):
                    entities["literals"].append(val)
    except Exception:
        pass
    return entities

def extract_entities(code, ext):
    if ext in [".js", ".jsx"]:
        return extract_entities_tree_sitter(code, parser_js)
    elif ext == ".ts":
        return extract_entities_tree_sitter(code, parser_ts)
    elif ext == ".py":
        return extract_entities_tree_sitter(code, parser)
    else:
        return defaultdict(list)

def process_commit_data(args):
    repo_id, commit = args
    lines = commit.splitlines()
    if not lines or not lines[0].startswith("commit") or "[bot]" in lines[1]:
        return []

    chash = lines[0].split()[1]
    date = lines[2] if len(lines) > 2 else ""

    # Extract commit message lines (lines after the 3rd line until "diff --git" or end)
    message_lines = []
    for line in lines[3:]:
        if line.startswith("diff --git"):
            break
        message_lines.append(line.strip())

    message = " ".join(m for m in message_lines if m)
    entries = []
    diffs = re.split(r'diff --git a/', commit)
    if len(diffs) > 1000:
        return entries

    # if "Merge pull request #270 from HareshKarnan/master" in message:
    #     print(diffs)

    for diff in diffs[1:]:
        lines = diff.splitlines()
        if not lines:
            continue
        # print("Number of lines: ", len(lines))
        fpath = lines[0].split()[0]
        ext = Path(fpath).suffix.lower()
        if ext not in target_exts:
            continue
        if len(lines)>1000:
            continue
        patch = "\n".join(line[1:] for line in lines[1:] if line.startswith("+"))
        try:
            ents = extract_entities(patch, ext)
            entries.append({
                "repo_id": repo_id,
                "date": date,
                "commit": chash,
                "message": message,
                "file": fpath,
                "ext": ext,
                "functions": ", ".join(ents.get("functions", [])),
                "classes": ", ".join(ents.get("classes", [])),
                "variables": ", ".join(ents.get("variables", [])),
                "literals": ", ".join(ents.get("literals", [])),
                "comments": ", ".join(ents.get("comments", [])),
                "docstrings": ", ".join(ents.get("docstrings", [])),
                "regex": ", ".join(ents.get("regex", [])),
            })
        except:
            pass
    # print("get entries ", len(entries))
    return entries


def read_commits_stream(file_path):
    # First, count the number of commits for tqdm
    # with open(file_path, encoding="utf-8", errors="replace") as f:
    #     commit_count = sum(1 for line in f if line.startswith("commit "))

    with open(file_path, encoding="utf-8", errors="replace") as f:
        commit = []
        for line in f:
            if line.startswith("commit ") and commit:
                yield ''.join(commit)
                commit = [line]
                # pbar.update(1)
            else:
                commit.append(line)
        if commit:
            yield ''.join(commit)
            # pbar.update(1)

# def process_large_log_file(fname):
#     if not fname.endswith(".txt"):
#         print("Only .txt files supported.")
#         return

#     file_path = os.path.join("logs", fname)
#     print(f"Processing file: {file_path}")

#     repo_id = Path(fname).stem
#     inputs = [(repo_id, commit) for commit in read_commits_stream(file_path)]
#     print("commit length", len(inputs))

#     with Pool(cpu_count()) as pool:
#         results = list(tqdm(pool.imap_unordered(process_commit_data, inputs), total=len(inputs), desc="Processing commits"))

#     flat_results = [entry for sublist in results for entry in sublist]
#     out_df = pd.DataFrame(flat_results)
#     out_file = f"{repo_id}_parsed.csv"
#     out_df.to_csv(out_file, index=False)
#     print(f"Saved results to {out_file}")

# if __name__ == "__main__":
#     file_to_process = "18742214.txt"  # replace with your actual log filename
#     process_large_log_file(file_to_process)


# def process_large_log_file(fname):
#     file_path = os.path.join("logs", fname)
#     size_bytes = os.path.getsize(file_path)
#     if size_bytes > 1000000000:
#         print(f"Skipping {fname} (size > 1GB)")
#         return

#     print(f"Processing file: {fname}")
#     repo_id = Path(fname).stem
#     inputs = [(repo_id, commit) for commit in read_commits_stream(file_path)]
#     # print(f"Total commits in {fname}: {len(inputs)}")

#     with Pool(cpu_count()) as pool:
#         results = list(tqdm(pool.imap_unordered(process_commit_data, inputs), total=len(inputs), desc=f"Processing {fname}"))

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

from multiprocessing import TimeoutError  # add this at the top if not already

def process_large_log_file(fname):
    file_path = os.path.join("logs", fname)
    size_bytes = os.path.getsize(file_path)
    if size_bytes > 1000000000:
        print(f"Skipping {fname} (size > 1GB)")
        return

    print(f"Processing file: {fname}")
    repo_id = Path(fname).stem
    inputs = [(repo_id, commit) for commit in read_commits_stream(file_path)]
    print("number of commit", len(inputs))
    results = []
    with Pool(cpu_count()) as pool:
        it = pool.imap_unordered(process_commit_data, inputs)
        for _ in tqdm(range(len(inputs)), desc=f"Processing {fname}"):
            try:
                res = it.next(timeout=60)  # 15 minutes timeout per task
                results.append(res)
            except TimeoutError:
                results.append([])
            except Exception:
                results.append([])

    flat_results = [entry for sublist in results for entry in sublist]
    if flat_results:
        os.makedirs("parsed_json", exist_ok=True)
        out_file = os.path.join("parsed_json", f"{repo_id}_parsed.json")
        with open(out_file, "w", encoding="utf-8") as f:
            import json
            json.dump(flat_results, f, ensure_ascii=False, indent=2)
        print(f"Saved results to {out_file}")
    else:
        print(f"No valid entries found in {fname}")



if __name__ == "__main__":
    log_dir = "logs"
    all_files = sorted(f for f in os.listdir(log_dir) if f.endswith(".txt"))

    for fname in all_files:
        # if "101081298" in fname:
        process_large_log_file(fname)

