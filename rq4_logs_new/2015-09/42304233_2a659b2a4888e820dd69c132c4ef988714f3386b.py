#
# Input: CSV file with list of friends for each user
# Output: CSV file with edges (pairs of nodes)
#

in_file_name="../data/typical_mpgu_users.csv"
out_file_name ="../data/typical_mpgu_pairs.csv"

friends = dict()
friend_pairs = []


def friends2edges():
    
    in_file = open(in_file_name, 'r', encoding = 'utf-8')
    data = in_file.readlines()
    in_file.close()

    out_file = open(out_file_name, 'w')

    for dataline in data:
        data_fields = dataline.split(";")
        id = data_fields[0].replace("\"","")
        friends_list = data_fields[5].rstrip().replace("\"","").split(", ")
        friends[id] = friends_list

    for user in friends :
        friends_list = friends[user]
        # out_file.write(str(user) + str(friends_list)+"\n")
        for id in friends_list:
            if (id in friends.keys()) and (user > id) :
                out_file.write(user + "\t" + id + "\n")
    
    out_file.close()
    
friends2edges()
