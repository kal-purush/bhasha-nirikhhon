import sys
import argparse
import os
import subprocess
import numpy as np

MAX_CORES = 256

def writelist(file, lista):
    with open(file, 'w') as f:
        for line in lista:
            f.write(str(line) + '\n')

def get_filename(path):
    """ returns filename from path"""
    lista = path.split('/')
    return lista[-1]


def build_sar_cpu_command(sa_file, cpu):
    l = ["sadf", sa_file, "-dh", "-P", cpu]
    return l


def parse_sar_cpu(sa_file, output_dir):
    base_cpu_path = os.path.join(output_dir, "cpu")
    for i in range(0, 1):
        cpu = str(i)
        # parse sar
        cmd = build_sar_cpu_command(sa_file, cpu)
        res = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                             universal_newlines=True)
        stdout = res.stdout
        stderr = res.stderr
        if stderr:
            print("Follwoing error occured while processing, exiting..")
            print(stderr)
            sys.exit()

        # format of sadf output
        # hostname;interval;timestamp;CPU;%user;%nice;%system;%iowait;%steal;%idle
        lines = stdout.strip().split("\n")
        parsed_data = []
        for line in lines[1:]:
            fields = line.split(";")
            entry = {
                "node": fields[0],
                "timestamp": fields[2],
                "cpu": float(fields[3]),
                "user": float(fields[4]),
                "nice": float(fields[5]),
                "sys": float(fields[6]),
                "iowait": float(fields[7]),
                "steal": float(fields[8]),
                "idle": float(fields[9]),
            }
            parsed_data.append(entry)

        # create directory
        save_dir = os.path.join(base_cpu_path, cpu)
        os.makedirs(save_dir, exist_ok=True)
        f = get_filename(sa_file)
        # user
        f_out = f'{save_dir}/{f}_user'
        user = [entry["user"] for entry in parsed_data]
        writelist(f_out, user)
        # sys
        f_out = f'{save_dir}/{f}_idle'
        sys = [entry["sys"] for entry in parsed_data]
        writelist(f_out, sys)
        # iowait
        f_out = f'{save_dir}/{f}_idle'
        iowait = [entry["iowait"] for entry in parsed_data]
        writelist(f_out, iowait)
        # idle
        f_out = f'{save_dir}/{f}_idle'
        idle = [entry["idle"] for entry in parsed_data]
        writelist(f_out, idle)




def main():
    parser = argparse.ArgumentParser(description='Split sar output into files')

    parser.add_argument('base_path', type=str,
                              help='Path to where sa* files reside')
    parser.add_argument('sa_file', type=str,
                              help='sa file to process, e.g. sa30')
    parser.add_argument('output_dir', type=str,
                              help='Directory to spit files')
    parser.add_argument('--debug',  action='store_true',
                        help='Enable debug statemetns')

    args = parser.parse_args()
    base_path = args.base_path
    sa_file = os.path.join(base_path, args.sa_file)
    output_dir = args.output_dir
    debug = args.debug


    if not os.path.exists(base_path):
        print("base_path does not exists, provide the correct path")
        sys.exit()
    if not os.path.exists(sa_file):
        print("sa_file does not exists, provide the correct sa_file")
        sys.exit()
    if not os.path.exists(output_dir):
        print("output_dir does not exist, I'll create it for you")
        os.makedirs(output_dir, exist_ok=True)

    # cpu = str(256)
    # subprocess.run(["sadf", sa_file, "-dh", "-P", cpu])

    if debug:
        print(base_path)
        print(sa_file)
        print(output_dir)

    parse_sar_cpu(sa_file, output_dir)
    # create_cpu_files(output_dir, sa_file)

if __name__ == '__main__':
    main()