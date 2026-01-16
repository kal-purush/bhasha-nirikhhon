import os
import shutil
import platform
from conanfile import IgeConan

def setEnv(name, value):
    if platform.system() == 'Windows':
        os.system(f'set {name}={value}')
    else:
        os.system(f'export {name}={value}')
    os.environ[str(name)] = str(value)

def safeRemove(rm_path):
    try:
        if os.path.isfile(rm_path):
            os.remove(rm_path)
        else:
            shutil.rmtree(rm_path)
    except:
        pass

def build(platform, arch):
    safeRemove('build')
    try:
        os.mkdir('build')
    except:
        pass
    os.chdir('build')
    ret_code = os.system(f'conan install --update .. --profile ../cmake/profiles/{platform}_{arch}')
    if ret_code != 0:
        exit(1)

    os.chdir('..')
    ret_code = os.system('conan build . --build-folder build')
    if ret_code != 0:
        exit(1)

    ret_code = os.system(f'conan export-pkg . {IgeConan.name}/{IgeConan.version}@ige/test --build-folder build --force')
    if ret_code != 0:
        exit(1)

def main():
    setEnv('CONAN_REVISIONS_ENABLED', '1')
    if platform.system() == 'Windows':
        build('windows', 'x86')
        build('windows', 'x86_64')
        build('android', 'x86')
        build('android', 'x86_64')
        build('android', 'armv7')
        build('android', 'armv8')
    elif platform.system() == 'Darwin':
        build('macos', 'x86_64')
        build('ios', 'armv8')
    ret_code = os.system(f'conan upload {IgeConan.name}/{IgeConan.version}@ige/test --remote ige-center  --all --check --confirm --retry 3 --retry-wait 60 --force')
    if ret_code != 0:
        exit(1)

if __name__ == "__main__":
    main()