def delete_begin(coordinate_begin, grid, gate_connections, gate_start, distances):
    # Define x, y and z coordinates of start and end gate
    x_coordinate_start = int(coordinate_begin[0])
    y_coordinate_start = int(coordinate_begin[1])
    z_coordinate_start = int(coordinate_begin[2])        

    # Define all 5 coordinates that surround current start coordinate
    x_coordinate_startcheck = x_coordinate_start + 1
    coordinate_1 = [x_coordinate_startcheck, y_coordinate_start, z_coordinate_start]
    x_coordinate_startcheck2 = x_coordinate_start - 1
    coordinate_2 = [x_coordinate_startcheck2, y_coordinate_start, z_coordinate_start]
    y_coordinate_startcheck = y_coordinate_start + 1
    coordinate_3 = [x_coordinate_start, y_coordinate_startcheck, z_coordinate_start]
    y_coordinate_startcheck2 = y_coordinate_start - 1
    coordinate_4 = [x_coordinate_start, y_coordinate_startcheck2, z_coordinate_start]
    z_coordinate_startcheck = z_coordinate_start + 1
    coordinate_5 = [x_coordinate_start, y_coordinate_start, z_coordinate_startcheck]
    
    # Saves all coordinates around current start coordinate in list
    coordinate_checks = [coordinate_1, coordinate_2, coordinate_3, coordinate_4, coordinate_5]
    
    counter = 0 
    # print("BEFOR BEGIN: ", len(gate_connections))
    for coordinate_check in coordinate_checks: 
        if not grid[tuple(coordinate_check)]:
            counter += 1
    deletewires = list()
    if counter == 5: 
        # print("COO CHECKS" ,coordinate_checks)
        # print("DELETE BEGIN")
        for coordinate_check in coordinate_checks:
            for keys in gate_connections:
                if tuple(coordinate_check) in gate_connections[keys] and gate_start not in keys:
                    for wire_coor in gate_connections[keys]:
                        grid[wire_coor] = True
                    deletewires.append((keys, len(gate_connections[keys])))
        for wire in deletewires:
            try: 
                del gate_connections[wire[0]]
            except: 
                pass
                # print("COULD NOT DELETE: ", wire)
            distances.append((wire[0], wire[1]))


        # print("AFTER BEGIN DEL: ", len(gate_connections))
    return grid, gate_connections, distances

def delete_end(coordinate_end, grid, gate_connections, gate_end, distances):
    x_coordinate_end = int(coordinate_end[0])
    y_coordinate_end = int(coordinate_end[1])
    z_coordinate_end = int(coordinate_end[2])

    # Define all 5 coordinates that surround current start coordinate
    x_coordinate_endcheck = x_coordinate_end + 1
    coordinate_1 = [x_coordinate_endcheck, y_coordinate_end, z_coordinate_end]
    x_coordinate_endcheck2 = x_coordinate_end - 1
    coordinate_2 = [x_coordinate_endcheck2, y_coordinate_end, z_coordinate_end]
    y_coordinate_endcheck = y_coordinate_end + 1
    coordinate_3 = [x_coordinate_end, y_coordinate_endcheck, z_coordinate_end]
    y_coordinate_endcheck2 = y_coordinate_end - 1
    coordinate_4 = [x_coordinate_end, y_coordinate_endcheck2, z_coordinate_end]
    z_coordinate_endcheck = z_coordinate_end + 1
    coordinate_5 = [x_coordinate_end, y_coordinate_end, z_coordinate_endcheck]
    
    # Saves all coordinates around current start coordinate in list
    coordinate_checks = [coordinate_1, coordinate_2, coordinate_3, coordinate_4, coordinate_5]
    
    counter = 0 
    # print("BEFOR END: ", len(gate_connections))
    for coordinate_check in coordinate_checks: 
        if not grid[tuple(coordinate_check)]:
            counter += 1
    deletewires = list()
    if counter == 5: 
        # print("COO CHECKS" ,coordinate_checks)
        # print("DELETE END")
        for coordinate_check in coordinate_checks:
            for keys in gate_connections:
                if tuple(coordinate_check) in gate_connections[keys] and gate_end not in keys:
                    for wire_coor in gate_connections[keys]:
                        grid[wire_coor] = True
                    deletewires.append((keys, len(gate_connections[keys])))
        for wire in deletewires:
            del gate_connections[wire[0]]
            distances.append((wire[0], wire[1]))

        # print("AFTER: ", len(gate_connections))
    return grid, gate_connections, distances