def look(room, roomnum):
    # Show the room name, description, and exits to players.
    exitstr = ""
    exit_list = ["n", "s", "e", "w"]
    for exit in exit_list:
        if room[roomnum][exit]["leadsto"] != -1:
            if room[roomnum][exit]["isclosed"]:
                exitstr += "<{}> ".format(exit)
            else:
                exitstr += "{} ".format(exit)

    print("{}\n{}\n\nExits: {}\n".format(room[roomnum]['name'],
                                         room[roomnum]['desc'],
                                         exitstr))