import mmap, os, signal
import argparse

hijo1 = 0
hijo2 = 0
file_directory = ""
lineb = bytes()
area = mmap.mmap(-1,1024)


def lee(nro, frame):
  area.seek(0)
  leido = area.read(1024)
  print(f'padre lee: {leido.decode()}')
  hijofn2(file_directory)
  os.kill(hijo2, signal.SIGUSR1)
  signal.pause()


def h2_save(nro, frame):
  # os.kill(hijo1, signal.SIGUSR1)
  signal.pause()


def h1_save(nro, frame):
  # padre_lee()
  pass


def hijofn2(file_directory):
  print(f'h2 notificado ...')
  area.seek(0)
  leido = area.read(1024)
  file2 = open(file_directory, "w+") 
  file2.write(leido.decode().upper())
  file2.flush()
  file2.close()
  os.kill(hijo1, signal.SIGUSR1)


def childs_function():
  while True: 
    line = input("Ingreso de caracteres: ")
    if "bye" == line:
      break
    lineb = bytes(line, 'utf-8')
    area.seek(0)
    area.write(lineb)
    os.kill(os.getppid(),signal.SIGUSR1)
    signal.pause()


def main(args):
  variables = vars(args)

  global file_directory
  file_directory = variables['f']
  pid = os.fork()
  hijo1 = os.getpid()

  if pid == 0:
    signal.signal(signal.SIGUSR1, h1_save)
    childs_function()
    
  else:
    signal.signal(signal.SIGUSR1, lee)
    pid2 = os.fork()
    if pid2 == 0:
      signal.signal(signal.SIGUSR1, h2_save)
      hijo2 = os.getpid()
      signal.pause()
    signal.pause()


if __name__ == "__main__":
  parser = argparse.ArgumentParser()
  parser.add_argument("-f", help='path donde se guarda el archivo')
  args = parser.parse_args()
  main(args)
