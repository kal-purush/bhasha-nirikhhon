import os
import yaml
import subprocess
from pathlib import Path
import mkdocs_gen_files

ROOT_PATH = "."
DOC_ROOT = "https://jessewebdotcom.github.io/self-hosted"
REPO_ROOT = "https://github.com/JesseWebDotCom/self-hosted/blob/main"

def write_file(contents, file_path):
    with open(file_path, 'w') as file:
        file.write(contents)

def is_ignored(file_path):
    try:
        command = ['git', 'check-ignore', file_path]
        result = subprocess.run(command, capture_output=True, text=True)
        return result.returncode == 0
    except FileNotFoundError:
        return True

def remove_extension(file_path):
    if os.path.isfile(file_path):
        base_name = os.path.basename(file_path)
        name, _ = os.path.splitext(base_name)
        return os.path.join(os.path.dirname(file_path), name)
    return file_path

def add_trailing_slash(variable):
    if not variable.endswith('/'):
        variable += '/'
    return variable       

def get_doc_url(file_path, doc_root):
    index = file_path.find("/environments")
    if index != -1:
        file_path = remove_extension(file_path)
        doc_path = f"{doc_root}/{file_path[index+1:]}"

        return add_trailing_slash(doc_path)
    return file_path

def get_repo_url(file_path, repo_root):
    index = file_path.find("/environments")
    if index != -1:
        return f"{repo_root}/{file_path[index+1:]}"

    return file_path    

def get_file_contents(input_path):
    content = ""
    if os.path.exists(input_path):
        with open(input_path, 'r') as input_file:
            content = input_file.read()
    return content    

class Container:
    def __init__(self, name, header):
        self.name = name
        self.header = header


class Compose:
    def __init__(self, file_path, doc_root, repo_root, show_secrets_values=False):
        self.file_path = file_path
        self.doc_root = doc_root
        self.repo_root = repo_root
        self.doc_url = get_doc_url(file_path, doc_root)
        self.repo_url = get_repo_url(file_path, repo_root)
        self.name = self._get_file_name()
        self._show_secrets_values = show_secrets_values
        self.header = ""
        self.description = ""
        self.containers = []

        self._parse_file()
        self.contents = self._load_contents()
        self.env = self._load_env()
        self.secrets = self._load_secrets()

    def _get_file_name(self):
        base_name = os.path.basename(self.file_path)
        name, _ = os.path.splitext(base_name)
        return name

    def _parse_file(self):
        with open(self.file_path, "r") as f:
            lines = f.readlines()

            self.header = self._extract_header(lines)
            self.description = self.header.split("\n")[0]

            data = yaml.safe_load("".join(lines))
            services = data.get("services", {})
            self.containers = self._extract_containers(services, lines)

    def _extract_header(self, lines):
        header = ""
        for line in lines:
            if line.strip().startswith("#"):
                header += line.strip().lstrip("#").strip() + "\n"
            else:
                break

        return header.rstrip()

    def _extract_containers(self, services, lines):
        containers = []
        container_keys = services.keys()

        for i, line in enumerate(lines):
            if line.strip().startswith(tuple(container_keys)):
                container_name = line.strip().rstrip(":")
                container_header = self._extract_container_header(lines[i + 1 :])
                containers.append(Container(container_name, container_header))

        return containers

    def _extract_container_header(self, lines):
        header = ""
        for line in lines:
            if line.strip().startswith("#"):
                header += line.strip().lstrip("#").strip() + "\n"
            else:
                break

        return header.rstrip()

    def _load_contents(self):
        if os.path.isfile(self.file_path):
            with open(self.file_path, "r") as f:
                return f.read()
        return None

    def _load_env(self):
        env_path = self.file_path.replace(".yml", ".env")
        if is_ignored(env_path):
            return None  

        if os.path.isfile(env_path):
            with open(env_path, "r") as f:
                return Env(f.read(), env_path, self.repo_root)
        return None

    def _load_secrets(self):
        secrets_path = self.file_path.replace(".yml", ".secrets.example")
        if self._show_secrets_values:
            secrets_path = self.file_path.replace(".yml", ".secrets")

        if is_ignored(secrets_path):
            return None

        if os.path.isfile(secrets_path):
            with open(secrets_path, "r") as f:
                return Secrets(f.read(), secrets_path, self.repo_root)
    
        return None


class Stack:
    def __init__(self, name, file_path, doc_root):
        self.name = name
        self.file_path = file_path
        self.doc_root = doc_root
        self.composes = []
        self.doc_url = get_doc_url(file_path, doc_root)

class Environment:
    def __init__(self, name, file_path, doc_root):
        self.name = name
        self.file_path = file_path
        self.doc_root = doc_root
        self.stacks = []
        self.doc_url = get_doc_url(file_path, doc_root)

class Env:
    def __init__(self, contents, file_path, repo_root):
        self.contents = contents
        self.file_path = file_path
        self.repo_url = get_repo_url(file_path, repo_root)


class Secrets:
    def __init__(self, contents, file_path, repo_root):
        self.contents = contents
        self.file_path = file_path
        self.repo_url = get_repo_url(file_path, repo_root)


class Environments:
    def __init__(self, root_path, doc_root, repo_root, show_secrets_values=False):
        self.root_path = root_path
        self.repo_root = repo_root
        self.file_path = os.path.join(self.root_path, "environments")
        self.doc_root = doc_root
        self.doc_url = get_doc_url(self.file_path, doc_root)        
        self.show_secrets_values = show_secrets_values
        self.environments = self._get_environments()

    def _get_environments(self):
        environments = []

        if not os.path.isdir(self.file_path):
            return environments

        for environment_name in os.listdir(self.file_path):
            environment_path = os.path.join(self.file_path, environment_name)
            if not os.path.isdir(environment_path):
                continue
            if is_ignored(environment_path):
                continue                

            environment = Environment(environment_name, environment_path, self.doc_root)

            for stack_name in os.listdir(environment_path):
                stack_path = os.path.join(environment_path, stack_name)
                if not os.path.isdir(stack_path):
                    continue
                if is_ignored(stack_path):
                    continue                

                stack = Stack(stack_name, stack_path, self.doc_root)

                for compose_file in os.listdir(stack_path):
                    if not compose_file.endswith(".yml"):
                        continue

                    compose_path = os.path.join(stack_path, compose_file)
                    if is_ignored(compose_path):
                        continue    
                                            
                    compose = Compose(compose_path, self.doc_root, self.repo_root, self.show_secrets_values)
                    stack.composes.append(compose)

                environment.stacks.append(stack)

            environments.append(environment)

        return environments

def generate_page(content, input_path):
    """Generate a page with the provided content and input path."""
    output_path = Path(input_path).with_suffix(".md")

    with mkdocs_gen_files.open(output_path, "w") as output_file:
        print(content, file=output_file)

    mkdocs_gen_files.set_edit_path(output_path, input_path)

def create_containers_table(data):
    table = "<table width=100%>"
    table += "<tr>" + "".join(f"<th>{header}</th>" for header in ["stack", "container", "description"]) + "</tr>"
    for data_row in data:
        table += f"<tr><td nowrap>{data_row[0]}</td><td nowrap>{data_row[1]}</td><td>{data_row[2]}</td></tr>" 
    table += "</table>"
    return table

def get_dashboard_image(name, width, url, show_name=False):
    replacements = {
        "airconnect": "chromecast",
        "cleanarr_movies": "benotes",
        "cleanarr_shows": "benotes",
        "homepage-for-tesla": "teslamate",
        "homepage_minimal": "homepage",
        "homepage_full": "homepage",
        "adguardhome": "adguard-home",
        "whoogle": "whooglesearch",
        "socket-proxy": "docker",
        "cloudflare-ddns": "cloudflare",
    }

    icon = next((replacements[find] for find in replacements if name == find), name)
    image = f"<img style='vertical-align:middle;padding-right: 5px' width={width} alt='{name}' title='{name}' src='https://cdn.jsdelivr.net/gh/walkxcode/dashboard-icons/png/{icon}.png'>"
    if url:
        image = f"<a href='{url}'>{image}{name if show_name else ''}</a>"

    return image

def get_code_block(title, tag, contents, url, description=""):
    if description !="":
        description += "\n"
    short_url = url.replace(REPO_ROOT, "")
    return f"## {title}\n{description}```{tag}\n\n{contents}\n```\n<small><a href='{url}'>View Source: {short_url}</a></small>\n"

def build_code_block(title, tag, relative_path, description):
    file_path = f"{ROOT_PATH}{relative_path}"
    repo_path = f"{REPO_ROOT}{relative_path}"
    if os.path.exists(file_path):
        return get_code_block(title, tag, get_file_contents(file_path), repo_path, description)
    return ""


environments_core = Environments(ROOT_PATH, DOC_ROOT, REPO_ROOT, show_secrets_values=False)
environments = environments_core.environments

# Iterate through environments, stacks, and composes
environments_table_rows = []
environments_summary_data = []
for environment in environments:

    containers_summary_data = []
    containers_image_bar = []
    for stack in environment.stacks:

        for compose in stack.composes:
            containers_summary_data.append([stack.name, get_dashboard_image(compose.name, "20px", compose.doc_url, True), compose.description])
            containers_image_bar.append(get_dashboard_image(compose.name, "20px", compose.doc_url, False))

            # create the compose page
            compose_index_path = f"{remove_extension(compose.file_path)}.md"
            compose_index_content = (
                f"# {get_dashboard_image(compose.name, '30px', compose.doc_url, True)}\n\n"
                f"{compose.description}"
                "\n## Containers\n"
                )
            for container in compose.containers:
                compose_index_content += f"### {container.name}\n{container.header}\n"
            compose_index_content += get_code_block(f"{container.name}.yml", "yaml", compose.contents, compose.repo_url, f"Primary docker compose file for {container.name}")
            if compose.env and compose.env.contents:
                compose_index_content += get_code_block(f"{container.name}.env", "ini", compose.env.contents, compose.env.repo_url, f"Primary environment file for {container.name}")
            if compose.secrets and compose.secrets.contents:
                compose_index_content += get_code_block(f"{container.name}.secrets", "ini", compose.secrets.contents, compose.secrets.repo_url, f"Primary secrets file for {container.name}")
            generate_page(compose_index_content, compose_index_path)


    environments_table_rows.append([f"<a href='{environment.doc_url}'>{environment.name.title()}</a>", len(containers_image_bar)])

    environment_index_path = f"{environment.file_path}/index.md"

    # create environment summary for home page
    environment_summary = (
        f"### <a href='{environment.doc_url}'>{environment.name.title()}</a><br>"
        f"{''.join(containers_image_bar)}\n\n"
        f"**{len(containers_image_bar)}** container(s): "
        f"{get_file_contents(environment_index_path).replace('## ', '#### ')}"
    )
    environments_summary_data.append(environment_summary)

    # create environment overview page
    environment_index_content = (
        "# Overview\n\n"
        f"{get_file_contents(environment_index_path)}"
        "\n## Containers\n"
        f"{''.join(containers_image_bar)}\n"
        f"{create_containers_table(containers_summary_data)}"
        )

    stacks_file_path = f"{environment.file_path}/stacks.txt"
    if os.path.exists(stacks_file_path):
        stacks_repo_url = f"{environment.file_path}/stacks.txt"
        environment_index_content += get_code_block("stacks.txt", "txt", get_file_contents(stacks_file_path), stacks_repo_url, f"Stacks used when composing an entire environment (ex. `tools/compose.sh {environment.name} up`)")

    generate_page(environment_index_content, environment_index_path)

# create environments overview page
environments_index_path = f"{environments_core.file_path}/index.md"
environments_index_content = (
    "# Overview\n\n<table width=100%><tr?\><th>Environment</th><th>Containers</th></tr>"
    )

for row in environments_table_rows:
    environments_index_content += f"<tr><td>{row[0]}</td><td>{row[1]}</td></tr>"
environments_index_content += "</table>\n"

environments_index_content += build_code_block("common-services.yml", "yml", "/environments/common-services.yml", "Common docker compose configurations that are imported using the `extends` keyword.")
environments_index_content += build_code_block(".env", "ini", "/environments/.env", "Environment variables that apply to all environments.")

generate_page(environments_index_content, environments_index_path)

# create my approach page
my_approach_index_path = f"{ROOT_PATH}/my_approach.md"

generate_page(get_file_contents(my_approach_index_path), my_approach_index_path)

# create home page    
home_index_path = f"{ROOT_PATH}/index.md"
home_index_content = (""
    "<h1 align='center'>"
    "<img width=75px src='https://jessewebdotcom.github.io/self-hosted/images/servers-svgrepo-com.svg'>"
    "<br />"
    "Self Hosted"
    "</h1>\n"
    f"This repo details my self-hosted environments, the services I run, and my approach to build and manage everything.<br><br>"
    "<span align='center'><a href='https://jesseweb.com/self-hosting/what-is-self-hosting/'>What is self hosting?</a>&nbsp;·&nbsp;"
    f"<a href='{DOC_ROOT}/my_approach/'>My approach</a></span>\n\n"
    f"## Environments\n{''.join(environments_summary_data)}"    
    )
generate_page(home_index_content, home_index_path)

# overwrite README
readme_path = f"{ROOT_PATH}/README.md"

write_file(home_index_content, readme_path)