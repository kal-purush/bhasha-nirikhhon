import json
import logging
from argparse import ArgumentParser
from pathlib import Path
import urllib.request

import yaml
import jinja2

BASE_PKG_NAME = "jupyterlab-language-pack"
BASE_URL = "https://pypi.org/pypi/{name}/json"

def main(version: str, dry_run: bool = True) -> None:
    recipe_path = Path("recipe/meta.yaml")
    raw_content = recipe_path.read_text()

    to_replace = {}

    tpl = jinja2.Environment().from_string(raw_content)
    current_recipe = tpl.render(locale="en-US", PYTHON="python")
    recipe = yaml.load(current_recipe, Loader=yaml.CLoader)
    to_replace = {
        recipe["package"]["version"]: version
    }
    
    for src in recipe["source"]:
        language = src["folder"]
        name = "-".join((BASE_PKG_NAME, language))
        logging.info(f"Get information for {name}@{version}.")
        try:
            with urllib.request.urlopen(BASE_URL.format(name=name)) as response:
                data = json.loads(response.read())
                for pkg in data["releases"][version]:
                    if pkg["packagetype"] == "sdist":
                        to_replace[src["sha256"]] = pkg["digests"]["sha256"]
        except Exception as e:
            logging.error("Fail to get information.", exc_info=e)
    
    new_content = raw_content
    for orig, modif in to_replace.items():
        new_content = new_content.replace(orig, modif, 1)
    if dry_run:
        print(new_content)
    else:
        recipe_path.write_text(new_content)

if __name__ == "__main__":
    parser = ArgumentParser(description="Bump package in recipe")
    parser.add_argument('version', help="Target version.")
    parser.add_argument('--dry-run', action='store_true', help="Print new recipe to stdout instead of overriding.")
    args = parser.parse_args()
    main(args.version, args.dry_run)