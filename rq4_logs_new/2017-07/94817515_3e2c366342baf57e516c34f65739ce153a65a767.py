import json
import os

json_dir = "E:\\fifth week\\Randy_data"
json_files = os.listdir(json_dir)

csv_op_dir = "E:\\fifth week\\csv_output"




def process_file(filename, outputfilename):
    filename = json_dir + os.sep + filename
    input_file = open(filename,'r')
    input_data = json.load(input_file)

    csv_list = []

    headers = ["ip_addr_player_id","event_name","level","time_to_finish","time_to_turn_gear","points_earned","stars_earned","local_time","moves"]
    csv_list.append(headers)

    def make_it_a_csv_row(ip_dict):
        op_list = []
        op_list.append(ip_dict["ip_addr_player_id"])
        op_list.append(ip_dict["event_name"])
        op_list.append(ip_dict["level"])
        op_list.append(ip_dict["time_to_finish"])
        op_list.append(ip_dict["time_to_turn_gear"])
        op_list.append(ip_dict["points_earned"])
        op_list.append(ip_dict["stars_earned"])
        op_list.append(ip_dict["local_time"])
        op_list.append(ip_dict["moves"])
        csv_list.append(op_list)

    limit_map = {}
    limit_map["moves"] = ' '
    limit_map["event_name"] = "'"
    limit_map["stars_earned"] = " "
    limit_map["points_earned"] = " "
    limit_map["time_to_finish"] = "'"
    limit_map["local_time"] = "'"
    limit_map["time_to_turn_gear"] = "'"
    limit_map["level"] = "'"


    def get_value(input, key):
        begin_index = 0
        try:
            begin_index = input.index(key)
        except:
            return "error"
        if begin_index < 0 :
            return " "
        begin_index = begin_index + len(key) + 1

        if(limit_map[key] == "'"):
            begin_index = begin_index + 1

        subs = input[begin_index: len(input)]
        end_index = 0
        try:
            end_index = subs.index(limit_map[key])
        except:
            end_index = len(subs)
        value = subs[0: end_index]
        return value


    import re

    for input_row in input_data:
        op_dict = {}

        row = input_row["m"]

        #ip address
        regex_ip_addr = r'\d+\.\d+\.\d+\.\d+'
        ip_addr = re.match(regex_ip_addr, row)[0]

        # player id
        rest = re.sub(regex_ip_addr, "", row)
        rest_parts = rest.split(" ")
        #print(rest_parts)
        player_id = rest_parts[1] + " " + rest_parts[2]
        op_dict["ip_addr_player_id"] = ip_addr + " " + player_id


        # convert the rest to key value pair
        rest = rest.replace(player_id, "")
        rest = rest.strip()


        # rest of the things
        look_up_keys = limit_map.keys()
        for key in look_up_keys:
            value = get_value(rest, key)
            if(value == "error"):
                print("!!!")
            op_dict[key]= value

        make_it_a_csv_row(op_dict)


    print(len(csv_list))


    import  csv
    outputfilename = csv_op_dir + os.sep + outputfilename + ".csv"
    with open(outputfilename,"w", newline='') as f:
        wr = csv.writer(f)
        wr.writerows(csv_list)





# Execute in directory
for json_file in json_files:
    oput = json_file + "_csv"
    process_file(json_file, oput)