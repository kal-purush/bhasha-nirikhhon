from vaderSentiment import vaderSentiment
import csv
import collections

if __name__ == '__main__':

    table = {}
    users = []
    threshold = 0.7 #must be greater than to be considered classified as neutral

    with open("data/userkey.txt", 'rt') as f:
        for line in f:
            users.append(line.strip())

    with open("data/sandy_all.txt", 'rt') as f:
        for line in f:
            tweet = line.split('\t')
            # print(tweet)
            user = tweet[0]

            # is this user in the master list
            if user not in users:
                continue

            if user not in table.keys():
                table[user] = {'total': 0, 'neutral': 0}

            # print('---')
            # print(tweet)
            ret = vaderSentiment.sentiment(tweet[1])
            # print(ret)

            if ret['neu'] >= threshold:
                table[user]['neutral'] += 1
            table[user]['total'] += 1

    with open('results.txt', 'w') as f:
        for user in sorted(table, key=lambda x: int(x)):
            # print(user, str(float(table[user]['neutral']) / float(table[user]['total'])))
            # f.write(user + ' ' + str(float(table[user]['neutral']) / float(table[user]['total'])) + '\n')
            f.write(str(float(table[user]['neutral']) / float(table[user]['total'])) + '\n')