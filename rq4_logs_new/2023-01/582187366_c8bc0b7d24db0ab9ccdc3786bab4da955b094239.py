matrix = [[None for j in range(6)] for i in range(10)]
pad_start_array = [[None for j in range(6)] for i in range(10)]
pad_size_array = [[None for j in range(6)] for i in range(10)]
width = 16

# Prompt the user for the number of processes
def get_num_processes():
  while True:
    num_processes = int(input("Enter the number of processes: "))
    print()
    if 3 <= num_processes <= 10:
      return num_processes

# Prompt the user for the details of each process
def get_process_details(num_processes):
    for i in range(num_processes):
        matrix[i][0] = input("Enter Process Name: ".format(i + 1))
        matrix[i][1] = int(input("Enter Arrival Time: ".format(i + 1)))
        matrix[i][2] = int(input("Enter Burst Time: ".format(i + 1)))
        matrix[i][3] = int(input("Enter Priority: ".format(i + 1)))
        print()
    return matrix

def print_matrix(num_processes,matrix):
    for i in range(num_processes):
        for j in range(6):
            print(matrix[i][j], end =' ')
        print()

def count_digit(num):
    return len(str(num))

def find_padding(num_col,num_processes,matrix):
    for i in range(num_processes):
        for j in range(num_col):
            pad_size_array[i][j] = width - count_digit(matrix[i][j])
            pad_start_array[i][j] = count_digit(matrix[i][j]) + pad_size_array[i][j] // 2


def show_input_table(num_processes,matrix):
    find_padding(4,num_processes,matrix)
    line = '-' * 67
    print(f"+{line}+")
    print("|    Process     |   Arrival Time   |  Burst Time  |    Priority    |")
    print(f"+{line}+")
    for i in range(num_processes):
        temp_array = ['' for j in range(4)]
        for j in range(4):
            temp = f"{matrix[i][j]:>{pad_start_array[i][j]}}"
            temp_array[j] = f"{temp:<{width}}"
        print(f"|{temp_array[0]}|{temp_array[1]}|{temp_array[2]}|{temp_array[3]}|")
    print(f"+{line}+")

def outputTable(num_processes,matrix):
    find_padding(6,num_processes,matrix)
    line = "-" * 101 # draw line
    print("+" + line + "+")
    print("|  Process  |  Arrival Time  |  Burst Time  |  Priority  |  Turnaround Time  |  Waiting Time  |")
    print("+" + line + "+")
    for i in range(num_processes):
        temp_array = ["" for j in range(6)]
        for j in range(6):
            temp = f"{matrix[i][j]:>{pad_start_array[i][j]}}"
            temp_array[j] = f"{temp:<{width}}"
        print(f"|{temp_array[0]}|{temp_array[1]}|{temp_array[2]}|{temp_array[3]}|{temp_array[4]}|{temp_array[5]}|")
    print("+" + line + "+")


def sortSequenceByAT(num_processes,matrix):
    for i in range(num_processes):
        for j in range(num_processes - i - 1):
            if matrix[j][1] > matrix[j + 1][1]:
                for k in range(5):
                    temp = matrix[j][k]
                    matrix[j][k] = matrix[j + 1][k]
                    matrix[j + 1][k] = temp

def calculateTotalCount(num_processes,matrix):
    totalCountTime = matrix[0][1]
    for i in range(num_processes):
        totalCountTime += matrix[i][2]
    return totalCountTime

def findATMinimum(num_processes,matrix):
    min_value =matrix[0][1]
    for i in range(1, num_processes):
        if matrix[i][1] < min_value:
            min_value = matrix[i][1]
    return min_value

def sortSequenceForSameAT(matrix,lastIndex):
    for i in range(lastIndex + 1):
        for j in range(lastIndex - i):
            if matrix[j][2] > matrix[j + 1][2]:
                for k in range(5):
                    temp = matrix[j][k]
                    matrix[j][k] = matrix[j + 1][k]
                    matrix[j + 1][k] = temp

def sortSequenceByBT(num_processes,matrix):
    outputArray = [[0 for j in range(6)] for i in range(num_processes)]
    waitingQueue = [0 for i in range(num_processes)]
    finishTimeArray = [0 for i in range(num_processes)]

    totCT = calculateTotalCount(num_processes,matrix)  # find total count time
    howManyDone = 0  # output array's row index
    countDown = None

    # to find who may become the first process
    lastIndex = 0
    for i in range(num_processes):
        if matrix[i][1] == findATMinimum(num_processes, matrix):
            lastIndex = i

    if lastIndex > 0:  # if more than 1 same first arrival time
        sortSequenceForSameAT(matrix,lastIndex)

    # add first process into output array
    for j in range(len(matrix[0])):  # copy all columns into new row
        outputArray[howManyDone][j] = matrix[0][j]

    countDown = matrix[0][2]  # first burst time
    nextIdToExecute = matrix[0][0]

    # start counting / start loop
    time = matrix[0][1]
    while time < totCT:
        # check is there any processes arrived at this time
        for i in range(howManyDone, num_processes):
            if matrix[i][1] == time:  # if the process arrived then add it into waiting queue
                waitingQueue.append(matrix[i][0])  # add process id to waiting queue

        # if the previous process done executing
        if countDown == 0:
            finishTimeArray[howManyDone] = time

            # if there is process in waiting queue
            if waitingQueue:
                lowestBT = 10000

                j = 0
                while j < len(waitingQueue):
                    if nextIdToExecute == waitingQueue[j]:
                        waitingQueue.pop(j)
                    else:
                        j += 1

                for k in range(num_processes):  # loop whole input array
                    for l in range(len(waitingQueue)):  # see id matched or not
                        if matrix[k][0] == waitingQueue[l]:  # if id in waiting queue, find who has the lowest burst time
                            if matrix[k][2] < lowestBT:
                                lowestBT = matrix[k][2]
                                nextIdToExecute = matrix[k][0]

                countDown = lowestBT  # update execution time
                howManyDone += 1

                # add to output array
                for m in range(num_processes):
                    if nextIdToExecute == matrix[m][0]:
                        for n in range(6):  # copy all columns into new row
                            outputArray[howManyDone][n] = matrix[m][n]

        # executing
        countDown -= 1
        time += 1
        
    finishTimeArray[num_processes - 1] = totCT
    return outputArray,finishTimeArray

def calculateTime(num_processes,outputArray, finishTimeArray):
    totalTAT = 0
    totalWT = 0
    for i in range(num_processes):
        outputArray[i][4] = finishTimeArray[i] - outputArray[i][1] # turnaround time
        outputArray[i][5] = outputArray[i][4] - outputArray[i][2] # waiting time
        totalTAT += outputArray[i][4]
        totalWT += outputArray[i][5]
    avgTAT = totalTAT / num_processes
    avgWT = totalWT / num_processes
    return avgTAT, avgWT

def printAverageTime(avgTAT, avgWT):
    print("Average Turnaround Time: ", "{:.2f}".format(avgTAT))
    print("Average Waiting Time: ", "{:.2f}".format(avgWT))

def createProcessForGanttChart(num_processes, outputArray):
    process_for_gantt_chart = []
    for i in range(num_processes):
        process_name = str(outputArray[i][0])
        process_for_gantt_chart.append(process_name)
    return process_for_gantt_chart

def createTimeForGanttChart(num_processes, outputArray, finishTimeArray):
    timeForGanttChart = []
    timeForGanttChart.append(outputArray[0][1]) # first arrival time
    for i in range(num_processes):
        timeForGanttChart.append(finishTimeArray[i])
    return timeForGanttChart

def printGanttChart(ganttChart):
    newGanttChart = []
    newGanttChart.extend(ganttChart)
    for i in range(len(newGanttChart)-1, 0, -1):
        if newGanttChart[i] == newGanttChart[i-1]:
            newGanttChart.pop(i)
    formattedGanttChart = str(newGanttChart).replace(",", " | ").replace("[", "| ").replace("]", " |").strip()
    return formattedGanttChart

def printTime(time):
    newTime = []
    # add first value
    newTime.append(time[0])

    for i in range(1, len(time)):
        # Compare current to previous value
        if time[i-1] != time[i]:
            newTime.append(time[i])

    formattedTime = "      ".join(str(x) for x in newTime)

    return formattedTime

def showGanttChart(ganttChart, time):
    print("\nGantt Chart: \n")
    print(printGanttChart(ganttChart))
    print(printTime(time)) #print time without comma and bracket


if __name__ == '__main__':
  print("Non Preemptive Shorter Job First\n")
  num = get_num_processes()
  M = get_process_details(num)
  print("\n\nInput Table\n")
  show_input_table(num,M)
  sortSequenceByAT(num,M)
  otArr,finTimeArr = sortSequenceByBT(num,M)
  avgTAT,avgWT = calculateTime(num,otArr,finTimeArr)
  print("\nOutput Table\n")
  outputTable(num,otArr)
  pro4GC = createProcessForGanttChart(num,otArr)
  tm4GC = createTimeForGanttChart(num,otArr,finTimeArr)
  showGanttChart(pro4GC,tm4GC)
  tTAT = avgTAT * num
  tWT = avgWT * num
  print("\nTotal Turnaround Time: ", "{:.2f}".format(tTAT))
  print("Total Waiting Time: ", "{:.2f}".format(tWT))
  printAverageTime(avgTAT,avgWT)

  



  
  