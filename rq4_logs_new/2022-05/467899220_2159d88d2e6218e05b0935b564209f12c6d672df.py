import shutil
from pathlib import Path


# >>> Handling project owner
USERNAME = ""
try:
    import asyncio

    import neuro_sdk

    async def get_username() -> str:
        async with await neuro_sdk.get() as client:
            return client.username

    USERNAME = asyncio.run(get_username())

except Exception:
    import subprocess

    if shutil.which("neuro"):
        result = subprocess.run(
            ["neuro", "config", "show"], stdout=subprocess.PIPE, stderr=subprocess.PIPE
        )
        if result.returncode == 0:
            cli_output = result.stdout.decode().splitlines()
            for line in cli_output:
                if "user name" in line.lower():
                    USERNAME = line.split()[2]
if USERNAME:
    proj_file = Path("./.neuro/project.yml")
    content = proj_file.read_text()
    content = content.replace("# owner: {ownername}", f"owner: {USERNAME}")
    content = content.replace(
        "# role: {rolename}", f"role: {USERNAME}/projects/{{ cookiecutter.project_id }}"
    )
    proj_file.write_text("".join(content))
else:
    live_file = Path("./.neuro/live.yml")
    content = live_file.read_text()
    content = content.replace("/$[[ project.owner ]]/", "")
    live_file.write_text("".join(content))
# <<< Handling project owner