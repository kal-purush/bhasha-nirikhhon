import gzip

zin = gzip.open('../endomondoHR.json.gz', 'rb')

total_workout = 0

user_dic = {}

print("Reading data...")

for l in zin:
    total_workout += 1

    l = l.decode('ascii')
    dic = eval(l)

    user_id = dic['userId']

    if user_id in user_dic:
        user_dic[user_id] += 1
    else:
        user_dic[user_id] = 1
print("Done.\n")

print("# of workouts is {}".format(total_workout))
print("# of users is {}".format(len(user_dic)))
