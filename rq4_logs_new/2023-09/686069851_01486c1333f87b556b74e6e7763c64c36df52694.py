import subprocess

print("Start running on HPC")
which_python = "/home/giladgressel/hpc-demo-main/env/bin/python"
main_path = "/home/giladgressel/hpc-demo-main/main.py"
subprocess.run([which_python, main_path])
print("Done running on HPC")