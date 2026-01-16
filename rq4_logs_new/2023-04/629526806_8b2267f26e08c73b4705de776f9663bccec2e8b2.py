# Configuration file for the Sphinx documentation builder.
#
# For the full list of built-in configuration values, see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

import os
import subprocess

# -- Custom steps to make the subprojects available here ---------------------
# WARNING: This should be run in a clean environment
#   because it installs the packages downloaded here
# Ensure we have the latest versions, in the docs project root
conf_dir = os.path.dirname(os.path.abspath(__file__))
project_dir = os.path.dirname(conf_dir)
repo_urls = {
    "lompe": "https://github.com/klaundal/lompe.git"
}
repo_paths = {_name: os.path.join(project_dir, _name) for _name in repo_urls.keys()}
for _name in repo_urls.keys():
    _repo_url = repo_urls[_name]
    _repo_path = repo_paths[_name]
    if not os.path.exists(_repo_path):
        subprocess.run(["git", "clone", _repo_url, _repo_path])
    else:
        subprocess.run(["git", "-C", _repo_path, "fetch"])
        subprocess.run(["git", "-C", _repo_path, "checkout", "main"])
        subprocess.run(["git", "-C", _repo_path, "reset", "--hard", "origin/main"])
    subprocess.run(["git", "-C", _repo_path, "submodule", "update", "--init"])
# Install those projects
# for _name, _repo_path in repo_paths.items():
#     subprocess.run(["python", "-m", "pip", "install", "--editable", _repo_path])
lompe_path = repo_paths["lompe"]
subprocess.run(["python", "-m", "pip", "install", "--editable", f"{lompe_path}[extras]"])
# Make shorter link to the examples directory
examples_path_inside_lompe = os.path.join(lompe_path, "examples")
examples_shortcut = os.path.join(conf_dir, "examples")
if not os.path.exists(examples_shortcut):
    subprocess.run(["ln", "-s", examples_path_inside_lompe, examples_shortcut])

# -- Project information -----------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#project-information

project = "Lompe"
copyright = "2023, The Lompe developers"
author = "The Lompe developers"

# -- General configuration ---------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#general-configuration

extensions = [
    "sphinx.ext.autodoc",
    "sphinx.ext.autosummary",
    "sphinx.ext.napoleon",
    "myst_nb",
]

templates_path = ["_templates"]
exclude_patterns = ["_build", "Thumbs.db", ".DS_Store"]

# https://www.sphinx-doc.org/en/master/usage/extensions/autodoc.html
# https://www.sphinx-doc.org/en/master/usage/extensions/autosummary.html
autosummary_generate = True
autodoc_default_options = {
    "members": True,
    "undoc-members": True,
    "show-inheritance": True,
    "inherited-members": True,
}

# https://myst-parser.readthedocs.io/en/latest/configuration.html
# https://myst-nb.readthedocs.io/en/latest/configuration.html
myst_enable_extensions = [
    "amsmath",
    "colon_fence",
    "deflist",
    "dollarmath",
    "html_image",
]
nb_execution_mode = "off"

# -- Options for HTML output -------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#options-for-html-output

html_theme = "sphinx_book_theme"
html_static_path = ["_static"]