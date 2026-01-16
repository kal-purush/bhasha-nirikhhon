import git
import os
from pathlib import Path

repo = git.Repo(os.getcwd(), search_parent_directories=True)
REPO_ROOT = Path(repo.working_tree_dir)