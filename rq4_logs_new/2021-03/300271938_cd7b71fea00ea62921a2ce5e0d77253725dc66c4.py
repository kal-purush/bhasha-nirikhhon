import subprocess

from PyInstaller import __main__ as pyi_main


TEST_CODE = [
    "import ScoutSuite.api_run",
    "from ScoutSuite.providers.aws.provider import AWSProvider",
    "",
    "cloud_type = 'aws'",
    "ScoutSuite.api_run.run(provider=cloud_type,",
                           "aws_access_key_id='abc',",
                           "aws_secret_access_key='abc',",
                           "aws_session_token='abc')"
]


# Tests
# =====
# Test out the package by importing it, then running functions from it.
def test_pyinstaller_scoutsuite(tmp_path):
    app_name = "ScoutSuiteApp"
    workpath = tmp_path / "build"
    distpath = tmp_path / "dist"
    app = tmp_path / (app_name + ".py")
    app.write_text("\n".join(TEST_CODE))
    args = [
        # Place all generated files in ``tmp_path``.
        '--workpath', str(workpath),
        '--distpath', str(distpath),
        '--specpath', str(tmp_path),
        '--additional-hooks-dir', 'C:\\Users\\Vakaris\\Desktop\\ScoutSuite\\ScoutSuite\\__pyinstaller'
        '--clean',
        str(app),
    ]
    pyi_main.run(args)
    subprocess.run([str(distpath / app_name / app_name)], check=True)