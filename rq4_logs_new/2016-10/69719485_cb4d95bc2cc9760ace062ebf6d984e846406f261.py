# hiker.py
#

in_date = input('Enter the date [mm/dd/yy], or "q" to exit:')


while in_date != "q":
    in_times = input('Enter the times, seperated by commas:')

    in_times = str.split(in_times, ',')
    in_times_half = int(in_times[1]) - int(in_times[0])
    in_times_three_quarter = int(in_times[2]) - int(in_times[1])
    in_times_final = int(in_times[3]) - int(in_times[2])
    Total_time = int(in_times[0]) + int(in_times_half) + int(in_times_three_quarter) + int(in_times_final)
    Average_time = Total_time / 4
    print("1/4 mark:", in_times[0])
    print("1/2 mark:", in_times_half)
    print("3/4 mark:", in_times_three_quarter)
    print("Finish:  ", in_times_final)
    print("Total time:",Total_time)
    print("Avg interval: %d" % Average_time)

    in_date = input('Enter the date [mm/dd/yy], or "q" to exit:')