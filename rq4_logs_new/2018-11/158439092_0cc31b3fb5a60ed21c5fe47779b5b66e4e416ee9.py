import sys
from mpi4py import MPI
from os import listdir
from os.path import isfile, join
from datetime import datetime

def main():
 comm = MPI.COMM_WORLD
 rank = comm.Get_rank()
 n = comm.Get_size()
 
 directory = sys.argv[1]

 fna = [filename for filename in listdir(directory) if filename[-3:] == ".fa"]  
 
 if rank == 0:
  adn_to_arn(fna[0], directory)
 elif rank == 1:
  adn_to_arn(fna[1], directory)
 elif rank == 2:
  adn_to_arn(fna[2], directory)

def adn_to_arn(fna, directory):
 
 f = open(directory + '/' + fna, 'r')
 res = open('/opt/dna/results/' + fna + '.result', 'w')

 for line in f:

  if line[0] == '>': continue
  resLine = ""

  for i in line:
   if i == 'T':
    resLine += 'A'
   elif i == 'A':
    resLine += 'U'
   elif i == 'C':
    resLine += 'G'
   elif i == 'G':
    resLine += 'C'  
   else:
    resLine += i
   res.write(resLine)
 
 f.close()
 res.close()

start = datetime.now()  
main()
end = datetime.now()
total = end - start
total_time = total.seconds
print(total_time)