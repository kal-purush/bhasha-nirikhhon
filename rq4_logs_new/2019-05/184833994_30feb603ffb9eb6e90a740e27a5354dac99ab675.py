import time as time
import numpy as np
import random as random

def quickSort(alist):
   quickSortHelper(alist,0,len(alist)-1)

def quickSortHelper(alist,first,last):
   if first<last:

       splitpoint = partition(alist,first,last)

       quickSortHelper(alist,first,splitpoint-1)
       quickSortHelper(alist,splitpoint+1,last)

def random_array(n):
    array = [] 
    
    for i in range(0, n, 1): 
        array.append(np.random.randint(0, 100))
    return array

def partition(alist,first,last):
   pivotvalue = alist[first]

   leftmark = first+1
   rightmark = last

   done = False
   while not done:

       while leftmark <= rightmark and alist[leftmark] <= pivotvalue:
           leftmark = leftmark + 1

       while alist[rightmark] >= pivotvalue and rightmark >= leftmark:
           rightmark = rightmark -1

       if rightmark < leftmark:
           done = True
       else:
           temp = alist[leftmark]
           alist[leftmark] = alist[rightmark]
           alist[rightmark] = temp

   temp = alist[first]
   alist[first] = alist[rightmark]
   alist[rightmark] = temp


   return rightmark

global quick_avg
quick_avg = []
num_runs= 10
results = []

for i in range (num_runs):
    
    start_time = time.time()
    alist = random_array(100)
    quickSort(alist)
    end_time = time.time()
    time_elapsed = end_time - start_time
    results.append(time_elapsed)
    
a = sum(results)
average = (a/num_runs)
quick_avg.append(average)

for i in range (num_runs):
    
    start_time = time.time()
    alist = random_array(250)
    quickSort(alist)
    end_time = time.time()
    time_elapsed = end_time - start_time
    results.append(time_elapsed)
    
a = sum(results)
average = (a/num_runs)
quick_avg.append(average)

for i in range (num_runs):
    
    start_time = time.time()
    alist = random_array(500)
    quickSort(alist)
    end_time = time.time()
    time_elapsed = end_time - start_time
    results.append(time_elapsed)
    
a = sum(results)
average = (a/num_runs)
quick_avg.append(average)

for i in range (num_runs):
    
    start_time = time.time()
    alist = random_array(750)
    quickSort(alist)
    end_time = time.time()
    time_elapsed = end_time - start_time
    results.append(time_elapsed)
    
a = sum(results)
average = (a/num_runs)
quick_avg.append(average)

for i in range (num_runs):
    
    start_time = time.time()
    alist = random_array(1000)
    quickSort(alist)
    end_time = time.time()
    time_elapsed = end_time - start_time
    results.append(time_elapsed)
    
a = sum(results)
average = (a/num_runs)
quick_avg.append(average)

for i in range (num_runs):
    
    start_time = time.time()
    alist = random_array(1250)
    quickSort(alist)
    end_time = time.time()
    time_elapsed = end_time - start_time
    results.append(time_elapsed)
    
a = sum(results)
average = (a/num_runs)
quick_avg.append(average)

for i in range (num_runs):
    
    start_time = time.time()
    alist = random_array(2500)
    quickSort(alist)
    end_time = time.time()
    time_elapsed = end_time - start_time
    results.append(time_elapsed)
    
a = sum(results)
average = (a/num_runs)
quick_avg.append(average)

for i in range (num_runs):
    
    start_time = time.time()
    alist = random_array(3750)
    quickSort(alist)
    end_time = time.time()
    time_elapsed = end_time - start_time
    results.append(time_elapsed)
    
a = sum(results)
average = (a/num_runs)
quick_avg.append(average)

for i in range (num_runs):
    
    start_time = time.time()
    alist = random_array(5000)
    quickSort(alist)
    end_time = time.time()
    time_elapsed = end_time - start_time
    results.append(time_elapsed)
    
a = sum(results)
average = (a/num_runs)
quick_avg.append(average)

for i in range (num_runs):
    
    start_time = time.time()
    alist = random_array(6250)
    quickSort(alist)
    end_time = time.time()
    time_elapsed = end_time - start_time
    results.append(time_elapsed)
    
a = sum(results)
average = (a/num_runs)
quick_avg.append(average)

for i in range (num_runs):
    
    start_time = time.time()
    alist = random_array(7500)
    quickSort(alist)
    end_time = time.time()
    time_elapsed = end_time - start_time
    results.append(time_elapsed)
    
a = sum(results)
average = (a/num_runs)
quick_avg.append(average)

for i in range (num_runs):
    
    start_time = time.time()
    alist = random_array(8750)
    quickSort(alist)
    end_time = time.time()
    time_elapsed = end_time - start_time
    results.append(time_elapsed)
    
a = sum(results)
average = (a/num_runs)
quick_avg.append(average)

for i in range (num_runs):
    
    start_time = time.time()
    alist = random_array(10000)
    quickSort(alist)
    end_time = time.time()
    time_elapsed = end_time - start_time
    results.append(time_elapsed)
    
a = sum(results)
average = (a/num_runs)
quick_avg.append(average)


print(quick_avg)
