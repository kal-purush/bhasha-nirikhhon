def delete_wire(wires, coordinate_begin, itemnet, distances, gate_connections, allwires):
    wires = []
    x_coordinate_start = int(coordinate_begin[0])
    y_coordinate_start = int(coordinate_begin[1])
    z_coordinate_start = int(coordinate_begin[2])
    coordinate = coordinate_begin
    
    # Switch order of gates
    end_gate = itemnet[0]
    start_gate = itemnet[1]
    distances.append(((start_gate, end_gate), 2))
    print("hallo")
    print(distances)
    # Delete wire from gate connections dictionary
    del gate_connections[itemnet]
    deletelist = []
    # Delete blocking wire
    for i, item2 in enumerate(allwires):
        if item2.net == itemnet:
            print("DELETETOM")
            # print(allwires[i])
            deletelist.append(allwires[i])

    for delete_wire in deletelist:
        print("REALDELETE")
        allwires.remove(delete_wire)
    return wires, x_coordinate_start, y_coordinate_start, z_coordinate_start, coordinate, gate_connections, allwires