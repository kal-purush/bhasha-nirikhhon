def main():
    import csv
    f=open('q1.csv', encoding='cp949')
    data=csv.reader(f)
    header=next(data)

    sum_1 = 0
    sum_2 = 0
    sum_3 = 0
    count=0
    average_1 = 0
    average_1 = 0
    average_1 = 0

    for row in data:
        count += 1
        if row[2]=='' or row[3]=='' or row[4]=='':
            continue
        else:
            sum_1 = sum_1+float(row[2])
            sum_2 = sum_2+float(row[3])
            sum_3 = sum_3+float(row[4])

    average1 = sum_1 / count
    average2 = sum_2 / count
    average3 = sum_3 / count
    
    print("*** Annual Temperature Report for Seoul in 2022 ***")
    print("Average Temperature:",round(average1,2),"Celsius")
    print("Average Minimum Temperature:",round(average2,2),"Celsius")
    print("Average Maximum Temperature:",round(average3,2),"Celsius")
    f.close()

if __name__ == '__main__':
    main()

