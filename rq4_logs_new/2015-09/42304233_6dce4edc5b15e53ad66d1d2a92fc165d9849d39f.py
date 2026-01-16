import json
from os import listdir
from os.path import isfile, join

friends_num = dict()
friends = dict()
friends_in = dict()
edges = list()

#
# Get friends number and compute the social graph
#
def process_friends():
    
    dir_name = "friends"
    res_file = open("edges.txt", 'w')

    uids_files = [ f for f in listdir(dir_name) if isfile(join(dir_name,f)) ]
    uids = [ f.split('.')[0] for f in uids_files ]

    for uid in uids:
        data_file = open(join(dir_name, uid + ".json"), 'r')
        data = json.load(data_file)['response']
        data_file.close()
        friends[uid] = [str(d) for d in data]
        friends_num[uid] = len(data)

    for uid in uids:
        friends_in[uid] = 0
        for friend in friends[uid]:
            if (uids.count(friend) > 0):
                friends_in[uid] = friends_in[uid] + 1
                if (uid > friend):
                    res_file.write(uid + "\t" + friend + "\n")
                    edges.append([uid,friend])
                
    #print(friends_in)
    res_file.close()

#
# Compute the graph core
#
def graph_core():
    n = 10
    res_edges = edges

    while True :

        # first, compute the set of remaining nodes 
        total = []
        for edge in res_edges : 
            total = total + edge
        nodes = set(total)
        
        # second, remove edges of "weak" nodes
        safe_edges = []
        finish = True
        
        for edge in res_edges:
            if (total.count(edge[0]) >= n) and (total.count(edge[1]) >= n) :
                safe_edges.append(edge)
            else :
                finish = False
                
        if finish :
            break
        res_edges = safe_edges

    print(res_edges)
    print(len(res_edges))

#
# Get users personal data
#
def process_stat():
    
    dir_name = "stat"
    res_file = open("stat.txt", 'w')
    
    uids_files = [ f for f in listdir(dir_name) if isfile(join(dir_name,f)) ]

    for uid_file in uids_files:
        with open(join(dir_name,uid_file)) as data_file:    
            data = json.load(data_file)
        stat = data['response'][0]
        uid_str = str(stat['uid'])
        info = ["id"+uid_str, stat['domain'], stat['first_name'], stat['last_name'], stat['counters']['followers'], stat['photo_max'],friends_num[uid_str],friends_in[uid_str]]
        info_string = [str(i) for i in info ]
        res_file.write("\t".join(info_string) + "\n")
    
    print("Ok")
    res_file.close()

process_friends()
process_stat()
graph_core()