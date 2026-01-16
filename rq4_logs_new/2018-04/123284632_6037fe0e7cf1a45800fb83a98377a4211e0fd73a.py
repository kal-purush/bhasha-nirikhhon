import os
import subprocess
import shlex

def run(cmd):
    print("running:", cmd) 
    command = shlex.split(cmd)
    process = subprocess.Popen(command, stdout=subprocess.PIPE)
    process.wait()
    output, err = process.communicate()
    print("output:", str(output).replace('\n', '').replace('\r', ''))
    print("err:", str(err).replace('\n', '').replace('\r', ''))
    if process.returncode != 0:
        print("command failed:", cmd)  
        exit(1)

cwd = os.getcwd()
solution_folder = os.path.split(cwd)[0]
project_name = os.path.split(cwd)[1]
print('starting packing for:', project_name)

#Removing old package files
bin_folder = os.path.join(cwd, "bin")
for item in os.listdir(bin_folder):
    if item.endswith(".nupkg"):
        os.remove(os.path.join(bin_folder, item))

#restore and build
run("dotnet restore --no-cache")
run("dotnet build --no-restore")
  
#test if available
test_path = os.path.join(solution_folder, project_name + ".Tests")
print(test_path)
if os.path.exists(test_path):
    print('Test project found')
    os.chdir(test_path)
    run('dotnet test')
    os.chdir(cwd)

#create package
run(f'dotnet pack --no-dependencies --include-source --include-symbols --no-restore --no-build --output ./bin')

#push packages
os.chdir(bin_folder)
for item in os.listdir(bin_folder):
    if item.endswith(".nupkg"):
        run(f'dotnet nuget push {item} -s http://192.168.1.29:8624/nuget/millentrix -k owRopMzGVsu6zEQ0wdyS')

