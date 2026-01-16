from multiprocessing import Process
import os
import subprocess
import time

start = time.time()
source_path = 'Source'
result_path = 'Result'


def resize(files):
    for file in files:
        subprocess.run('convert {}/{} -resize 200 {}/{}'.format(source_path, file, result_path, file))


if __name__ == '__main__':
    files = os.listdir(path=source_path)
    os.mkdir('Result')
    p = Process(target=resize(files), args=(files,))
    p.start()
    p2 = Process(target=resize(files), args=(files,))
    p2.start()
    p3 = Process(target=resize(files), args=(files,))
    p3.start()
    p4 = Process(target=resize(files), args=(files,))
    p4.start()
    p.join()
    p2.join()
    p3.join()
    p4.join()
    elapsed = (time.time() - start)
    print(elapsed)
    pass
