import csv


def writetocsv(predict):
    with open("data/output.csv", "w", newline='') as myfile:
        wr = csv.writer(myfile)
        wr.writerow(["Sample_id", "Sample_label"])
        for i in range(0, len(predict)):
            wr.writerow([i + 1, str(predict[i].item())])