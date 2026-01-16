# %%
import subprocess
import json
import os

# %% Change to the root directory of the git repository
def find_git_root(path):
    try:
        result = subprocess.run(
            ['git', 'rev-parse', '--show-toplevel'],
            cwd=path,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            check=True
        )
        return result.stdout.strip()
    except subprocess.CalledProcessError:
        return None

git_root = find_git_root(os.getcwd())
if git_root is None:
    print("Not inside a Git repository.")
    exit(1)

os.chdir(git_root)

# %%
# Retrieve current environment packages
print("Getting current python packages")
json_result = subprocess.run(['conda', 'list', '--json'], stdout=subprocess.PIPE)
current_env_json = json_result.stdout.decode('utf-8')

# %%
# Parse the JSON data
try:
    current_versions = json.loads(current_env_json)
except json.JSONDecodeError as e:
    print(f"Error decoding JSON: {e}")
    current_versions = []

current_versions_sorted = sorted(current_versions, key=lambda x: x['name'])

# Save sorted JSON data to a file
current_versions_file = os.path.join(git_root, 'docker', 'metadata', '03-full_python_package_current.json')
os.makedirs(os.path.dirname(current_versions_file), exist_ok=True)

with open(current_versions_file, 'w') as file:
    json.dump(current_versions_sorted, file, indent=4)
print("Current python packages saved to 03-full_python_package_current.json")

# %%
# Run the dry-run command and capture the output
print("Checking for updates")
json_result = subprocess.run(['conda', 'update', '--all', '--dry-run', '--json'], stdout=subprocess.PIPE)
json_data = json_result.stdout.decode('utf-8')

# %%
# Parse the JSON data
try:
    updates = json.loads(json_data)
except json.JSONDecodeError as e:
    print(f"Error decoding JSON: {e}")
    updates = {}

# %%
# Create a dictionary of current packages
current_packages = {
    f"{pkg['name']}-{pkg['version']}": pkg
    for pkg in current_versions_sorted
}

# %%
# Create a dictionary of packages to be updated
update_packages = {
    f"{pkg['name']}-{pkg['version']}": pkg
    for pkg in updates.get('actions', {}).get('FETCH', [])
}

# %%
# Compare and report updates
comparison_results = []
for key, new_pkg in update_packages.items():
    package_name, new_version = key.rsplit('-', 1)
    current_key = next((k for k in current_packages if k.startswith(package_name)), None)
    if current_key:
        current_version = current_packages[current_key]['version']
        if current_version != new_version:
            comparison_results.append({
                'package': package_name,
                'current_version': current_version,
                'new_version': new_version
            })
    else:
        comparison_results.append({
            'package': package_name,
            'current_version': None,
            'new_version': new_version
        })

comparison_results_sorted = sorted(comparison_results, key=lambda x: x['package'])

# %%
# Save comparison results to a file
comparison_results_file = os.path.join(git_root, 'docker', 'metadata', '03-full_python_package_updates.json')

with open(comparison_results_file, 'w') as f:
    json.dump(comparison_results_sorted, f, indent=4)

print("Comparison results saved to 03-full_python_package_updates.json")