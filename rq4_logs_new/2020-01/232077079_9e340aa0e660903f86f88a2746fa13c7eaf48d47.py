"""
Linespacer.py

Python file with the functions for the first algorithm

Tom Kamstra, Julia Linde, Izhar Hamer
""" 
    
    
    
    
def Make_Wire(input_direction, coordinate_start, coordinate_end, step_x, step_y, gate_connections, gate_coordinates, wires, allwires): 
    if input_direction == 'X':
        direction_coordinate_start = coordinate_start[0]
        direction_coordinate_end = coordinate_end[0]
        otherdirection_coordinate_start = coordinate_start[1]
        otherdirection_coordinate_end = coordinate_end[1]

    elif input_direction = 'Y': 
        direction_coordinate_start = coordinate_start[1]
        direction_coordinate_end = coordinate_end[1]
        otherdirection_coordinate_start = coordinate_start[0]
        otherdirection_coordinate_end = coordinate_end[0]
     
    # Loop until x-coordinate from start gate equals x-coordinate from end gate
    while direction_coordinate_start != direction_coordinate_end:
        direction_coordinate_start = direction_coordinate_start + step_x
        coordinate = coordinate_start
        # Check for other gates or other wires
        if gate_connections:
            for key in gate_connections:
                selected_wires = gate_connections[key]
                # print("SELECTED WIRESSSSSS")
                # print(selected_wires)
                if coordinate in selected_wires or coordinate in gate_coordinates:
                    if coordinate != coordinate_end:
                        direction_coordinate_start = direction_coordinate_start - step_x
                        # z kan nu niet meerdere stappen omhoog/omlaag
                        z_coordinate_start = z_coordinate_start + 1
                        #checken of na deze stap geen gate zit
                        break
            coordinate = [x_coordinate_start, y_coordinate_start, z_coordinate_start]
            if gate_connections:
                for key in gate_connections:
                    selected_wires = gate_connections[key]
                    if coordinate in selected_wires or coordinate in gate_coordinates:
                        if coordinate != coordinate_end:
                            z_coordinate_start = z_coordinate_start - 1
                            # z kan nu niet meerdere stappen omhoog/omlaag
                            y_coordinate_start = y_coordinate_start + step_y
                            #checken of na deze stap geen gate zit
                            break
                coordinate = [x_coordinate_start, y_coordinate_start, z_coordinate_start]
                if gate_connections:
                    for key in gate_connections:
                        selected_wires = gate_connections[key]
                        if coordinate in selected_wires or coordinate in gate_coordinates:
                            if coordinate != coordinate_end:
                                y_coordinate_start = y_coordinate_start - step_y - step_y
                                # z kan nu niet meerdere stappen omhoog/omlaag
                                break
                    coordinate = [x_coordinate_start, y_coordinate_start, z_coordinate_start]
                    if gate_connections:
                        for key in gate_connections:
                            selected_wires = gate_connections[key]
                            if coordinate in selected_wires or coordinate in gate_coordinates:
                                if coordinate != coordinate_end:
                                    y_coordinate_start = y_coordinate_start + step_y
                                    # z kan nu niet meerdere stappen omhoog/omlaag
                                    x_coordinate_start = x_coordinate_start - step_x
                                    #checken of na deze stap geen gate zit
                                    break
                    elif coordinate in gate_coordinates and coordinate != coordinate_end:
                        y_coordinate_start = y_coordinate_start + step_y
                        # z kan nu niet meerdere stappen omhoog/omlaag
                        x_coordinate_start = x_coordinate_start - step_x
                        #checken of na deze stap geen gate zit
                elif coordinate in gate_coordinates and coordinate != coordinate_end:
                    y_coordinate_start = y_coordinate_start - step_y - step_y
                    # z kan nu niet meerdere stappen omhoog/omlaag
                    coordinate = [x_coordinate_start, y_coordinate_start, z_coordinate_start]
                    if gate_connections:
                        for key in gate_connections:
                            selected_wires = gate_connections[key]
                            if coordinate in selected_wires or coordinate in gate_coordinates:
                                if coordinate != coordinate_end:
                                    y_coordinate_start = y_coordinate_start + step_y
                                    # z kan nu niet meerdere stappen omhoog/omlaag
                                    x_coordinate_start = x_coordinate_start - step_x
                                    #checken of na deze stap geen gate zit
                                    break
                    elif coordinate in gate_coordinates and coordinate != coordinate_end:
                        y_coordinate_start = y_coordinate_start + step_y
                        # z kan nu niet meerdere stappen omhoog/omlaag
                        x_coordinate_start = x_coordinate_start - step_x
                        #checken of na deze stap geen gate zit
            elif coordinate in gate_coordinates and coordinate != coordinate_end:
                z_coordinate_start = z_coordinate_start - 1
                # z kan nu niet meerdere stappen omhoog/omlaag
                y_coordinate_start = y_coordinate_start + step_y
                #checken of na deze stap geen gate zit
                coordinate = [x_coordinate_start, y_coordinate_start, z_coordinate_start]
                if gate_connections:
                    for key in gate_connections:
                        selected_wires = gate_connections[key]
                        if coordinate in selected_wires or coordinate in gate_coordinates:
                            if coordinate != coordinate_end:
                                y_coordinate_start = y_coordinate_start + step_y
                                # z kan nu niet meerdere stappen omhoog/omlaag
                                x_coordinate_start = x_coordinate_start - step_x
                                #checken of na deze stap geen gate zit
                                break
                elif coordinate in gate_coordinates and coordinate != coordinate_end:
                    y_coordinate_start = y_coordinate_start + step_y
                    # z kan nu niet meerdere stappen omhoog/omlaag
                    x_coordinate_start = x_coordinate_start - step_x
                    #checken of na deze stap geen gate zit
        elif coordinate in gate_coordinates and coordinate != coordinate_end:
            x_coordinate_start = x_coordinate_start - step_x
            # z kan nu niet meerdere stappen omhoog/omlaag
            z_coordinate_start = z_coordinate_start + 1
            #checken of na deze stap geen gate zit
            coordinate = [x_coordinate_start, y_coordinate_start, z_coordinate_start]
            if gate_connections:
                for key in gate_connections:
                    selected_wires = gate_connections[key]
                    if coordinate in selected_wires or coordinate in gate_coordinates:
                        if coordinate != coordinate_end:
                            z_coordinate_start = z_coordinate_start - 1
                            # z kan nu niet meerdere stappen omhoog/omlaag
                            y_coordinate_start = y_coordinate_start + step_y
                            #checken of na deze stap geen gate zit
                            break
                coordinate = [x_coordinate_start, y_coordinate_start, z_coordinate_start]
                if gate_connections:
                    for key in gate_connections:
                        selected_wires = gate_connections[key]
                        if coordinate in selected_wires or coordinate in gate_coordinates:
                            if coordinate != coordinate_end:
                                y_coordinate_start = y_coordinate_start - step_y - step_y
                                # z kan nu niet meerdere stappen omhoog/omlaag
                                break
                    coordinate = [x_coordinate_start, y_coordinate_start, z_coordinate_start]
                    if gate_connections:
                        for key in gate_connections:
                            selected_wires = gate_connections[key]
                            if coordinate in selected_wires or coordinate in gate_coordinates:
                                if coordinate != coordinate_end:
                                    y_coordinate_start = y_coordinate_start + step_y
                                    # z kan nu niet meerdere stappen omhoog/omlaag
                                    x_coordinate_start = x_coordinate_start - step_x
                                    #checken of na deze stap geen gate zit
                                    break
                    elif coordinate in gate_coordinates and coordinate != coordinate_end:
                        y_coordinate_start = y_coordinate_start + step_y
                        # z kan nu niet meerdere stappen omhoog/omlaag
                        x_coordinate_start = x_coordinate_start - step_x
                        #checken of na deze stap geen gate zit
                elif coordinate in gate_coordinates and coordinate != coordinate_end:
                    y_coordinate_start = y_coordinate_start - step_y - step_y
                    # z kan nu niet meerdere stappen omhoog/omlaag
                    coordinate = [x_coordinate_start, y_coordinate_start, z_coordinate_start]
                    if gate_connections:
                        for key in gate_connections:
                            selected_wires = gate_connections[key]
                            if coordinate in selected_wires or coordinate in gate_coordinates:
                                if coordinate != coordinate_end:
                                    y_coordinate_start = y_coordinate_start + step_y
                                    # z kan nu niet meerdere stappen omhoog/omlaag
                                    x_coordinate_start = x_coordinate_start - step_x
                                    #checken of na deze stap geen gate zit
                                    break
                    elif coordinate in gate_coordinates and coordinate != coordinate_end:
                        y_coordinate_start = y_coordinate_start + step_y
                        # z kan nu niet meerdere stappen omhoog/omlaag
                        x_coordinate_start = x_coordinate_start - step_x
                        #checken of na deze stap geen gate zit
            elif coordinate in gate_coordinates and coordinate != coordinate_end:
                z_coordinate_start = z_coordinate_start - 1
                # z kan nu niet meerdere stappen omhoog/omlaag
                y_coordinate_start = y_coordinate_start + step_y
                #checken of na deze stap geen gate zit
                coordinate = [x_coordinate_start, y_coordinate_start, z_coordinate_start]
                if gate_connections:
                    for key in gate_connections:
                        selected_wires = gate_connections[key]
                        if coordinate in selected_wires or coordinate in gate_coordinates:
                            if coordinate != coordinate_end:
                                y_coordinate_start = y_coordinate_start + step_y
                                # z kan nu niet meerdere stappen omhoog/omlaag
                                x_coordinate_start = x_coordinate_start - step_x
                                #checken of na deze stap geen gate zit
                                break
                elif coordinate in gate_coordinates and coordinate != coordinate_end:
                    y_coordinate_start = y_coordinate_start + step_y
                    # z kan nu niet meerdere stappen omhoog/omlaag
                    x_coordinate_start = x_coordinate_start - step_x
                    #checken of na deze stap geen gate zit
        
        coordinate = [x_coordinate_start, y_coordinate_start, z_coordinate_start]
        wires.append(coordinate)
        print(coordinate)
        wire = classs.Wire(coordinate, connected_gate)
        allwires.append(wire)
        
        if y_coordinate_start < y_coordinate_end:
            step_y = 1
        elif y_coordinate_start > y_coordinate_end:
            step_y = -1
                
        if x_coordinate_start == x_coordinate_end and y_coordinate_start == y_coordinate_end:
            while z_coordinate_start != z_coordinate_end:
                z_coordinate_start = z_coordinate_start - 1
                coordinate = [x_coordinate_start, y_coordinate_start, z_coordinate_start]
                if gate_connections:
                    for key in gate_connections:
                        selected_wires = gate_connections[key]
                        if coordinate in selected_wires or coordinate in gate_coordinates:
                            if coordinate != coordinate_end:
                                z_coordinate_start = z_coordinate_start + 1
                                # z kan nu niet meerdere stappen omhoog/omlaag
                                y_coordinate_start = y_coordinate_start + step_y
                                #checken of na deze stap geen gate zit
                                break
                                # moet ook uit while loop breken!
                    coordinate = [x_coordinate_start, y_coordinate_start, z_coordinate_start]
                    if gate_connections:
                        for key in gate_connections:
                            selected_wires = gate_connections[key]
                            if coordinate in selected_wires or coordinate in gate_coordinates:
                                if coordinate != coordinate_end:
                                    y_coordinate_start = y_coordinate_start - step_y
                                    # z kan nu niet meerdere stappen omhoog/omlaag
                                    x_coordinate_start = x_coordinate_start + step_x
                                    #checken of na deze stap geen gate zit
                                    break
                                    # moet ook uit while loop breken!
                        coordinate = [x_coordinate_start, y_coordinate_start, z_coordinate_start]
                        if gate_connections:
                            for key in gate_connections:
                                selected_wires = gate_connections[key]
                                if coordinate in selected_wires or coordinate in gate_coordinates:
                                    if coordinate != coordinate_end:
                                        x_coordinate_start = x_coordinate_start - step_x - step_x
                                        # z kan nu niet meerdere stappen omhoog/omlaag
                                        break
                                        # moet ook uit while loop breken!
                            coordinate = [x_coordinate_start, y_coordinate_start, z_coordinate_start]
                            if gate_connections:
                                for key in gate_connections:
                                    selected_wires = gate_connections[key]
                                    if coordinate in selected_wires or coordinate in gate_coordinates:
                                        if coordinate != coordinate_end:
                                            x_coordinate_start = x_coordinate_start + step_x
                                            # z kan nu niet meerdere stappen omhoog/omlaag
                                            y_coordinate_start = y_coordinate_start - step_y
                                            #checken of na deze stap geen gate zit
                                            break
                                            # moet ook uit while loop breken!
                            elif coordinate in gate_coordinates and coordinate != coordinate_end:
                                x_coordinate_start = x_coordinate_start + step_x
                                # z kan nu niet meerdere stappen omhoog/omlaag
                                y_coordinate_start = y_coordinate_start - step_y
                                #checken of na deze stap geen gate zit
                        elif coordinate in gate_coordinates and coordinate != coordinate_end:
                            x_coordinate_start = x_coordinate_start - step_x - step_x
                            # z kan nu niet meerdere stappen omhoog/omlaag
                            coordinate = [x_coordinate_start, y_coordinate_start, z_coordinate_start]
                            if gate_connections:
                                for key in gate_connections:
                                    selected_wires = gate_connections[key]
                                    if coordinate in selected_wires or coordinate in gate_coordinates:
                                        if coordinate != coordinate_end:
                                            x_coordinate_start = x_coordinate_start + step_x
                                            # z kan nu niet meerdere stappen omhoog/omlaag
                                            y_coordinate_start = y_coordinate_start - step_y
                                            #checken of na deze stap geen gate zit
                                            break
                                            # moet ook uit while loop breken!
                            elif coordinate in gate_coordinates and coordinate != coordinate_end:
                                x_coordinate_start = x_coordinate_start + step_x
                                # z kan nu niet meerdere stappen omhoog/omlaag
                                y_coordinate_start = y_coordinate_start - step_y
                                #checken of na deze stap geen gate zit
                    elif coordinate in gate_coordinates and coordinate != coordinate_end:
                        y_coordinate_start = y_coordinate_start - step_y
                        # z kan nu niet meerdere stappen omhoog/omlaag
                        x_coordinate_start = x_coordinate_start + step_x
                        #checken of na deze stap geen gate zit
                        coordinate = [x_coordinate_start, y_coordinate_start, z_coordinate_start]
                        if gate_connections:
                            for key in gate_connections:
                                selected_wires = gate_connections[key]
                                if coordinate in selected_wires or coordinate in gate_coordinates:
                                    if coordinate != coordinate_end:
                                        x_coordinate_start = x_coordinate_start - step_x - step_x
                                        # z kan nu niet meerdere stappen omhoog/omlaag
                                        break
                                        # moet ook uit while loop breken!
                            coordinate = [x_coordinate_start, y_coordinate_start, z_coordinate_start]
                            if gate_connections:
                                for key in gate_connections:
                                    selected_wires = gate_connections[key]
                                    if coordinate in selected_wires or coordinate in gate_coordinates:
                                        if coordinate != coordinate_end:
                                            x_coordinate_start = x_coordinate_start + step_x
                                            # z kan nu niet meerdere stappen omhoog/omlaag
                                            y_coordinate_start = y_coordinate_start - step_y
                                            #checken of na deze stap geen gate zit
                                            break
                                            # moet ook uit while loop breken!
                            elif coordinate in gate_coordinates and coordinate != coordinate_end:
                                x_coordinate_start = x_coordinate_start + step_x
                                # z kan nu niet meerdere stappen omhoog/omlaag
                                y_coordinate_start = y_coordinate_start - step_y
                                #checken of na deze stap geen gate zit
                        elif coordinate in gate_coordinates and coordinate != coordinate_end:
                            x_coordinate_start = x_coordinate_start - step_x - step_x
                            # z kan nu niet meerdere stappen omhoog/omlaag
                            coordinate = [x_coordinate_start, y_coordinate_start, z_coordinate_start]
                            if gate_connections:
                                for key in gate_connections:
                                    selected_wires = gate_connections[key]
                                    if coordinate in selected_wires or coordinate in gate_coordinates:
                                        if coordinate != coordinate_end:
                                            x_coordinate_start = x_coordinate_start + step_x
                                            # z kan nu niet meerdere stappen omhoog/omlaag
                                            y_coordinate_start = y_coordinate_start - step_y
                                            #checken of na deze stap geen gate zit
                                            break
                                            # moet ook uit while loop breken!
                            elif coordinate in gate_coordinates and coordinate != coordinate_end:
                                x_coordinate_start = x_coordinate_start + step_x
                                # z kan nu niet meerdere stappen omhoog/omlaag
                                y_coordinate_start = y_coordinate_start - step_y
                                #checken of na deze stap geen gate zit
                elif coordinate in gate_coordinates and coordinate != coordinate_end:
                    z_coordinate_start = z_coordinate_start + 1
                    # z kan nu niet meerdere stappen omhoog/omlaag
                    y_coordinate_start = y_coordinate_start + step_y
                    #checken of na deze stap geen gate zit
                    coordinate = [x_coordinate_start, y_coordinate_start, z_coordinate_start]
                    if gate_connections:
                        for key in gate_connections:
                            selected_wires = gate_connections[key]
                            if coordinate in selected_wires or coordinate in gate_coordinates:
                                if coordinate != coordinate_end:
                                    y_coordinate_start = y_coordinate_start - step_y
                                    # z kan nu niet meerdere stappen omhoog/omlaag
                                    x_coordinate_start = x_coordinate_start + step_x
                                    #checken of na deze stap geen gate zit
                                    break
                                    # moet ook uit while loop breken!
                        coordinate = [x_coordinate_start, y_coordinate_start, z_coordinate_start]
                        if gate_connections:
                            for key in gate_connections:
                                selected_wires = gate_connections[key]
                                if coordinate in selected_wires or coordinate in gate_coordinates:
                                    if coordinate != coordinate_end:
                                        x_coordinate_start = x_coordinate_start - step_x - step_x
                                        # z kan nu niet meerdere stappen omhoog/omlaag
                                        break
                                        # moet ook uit while loop breken!
                            coordinate = [x_coordinate_start, y_coordinate_start, z_coordinate_start]
                            if gate_connections:
                                for key in gate_connections:
                                    selected_wires = gate_connections[key]
                                    if coordinate in selected_wires or coordinate in gate_coordinates:
                                        if coordinate != coordinate_end:
                                            x_coordinate_start = x_coordinate_start + step_x
                                            # z kan nu niet meerdere stappen omhoog/omlaag
                                            y_coordinate_start = y_coordinate_start - step_y
                                            #checken of na deze stap geen gate zit
                                            break
                                            # moet ook uit while loop breken!
                            elif coordinate in gate_coordinates and coordinate != coordinate_end:
                                x_coordinate_start = x_coordinate_start + step_x
                                # z kan nu niet meerdere stappen omhoog/omlaag
                                y_coordinate_start = y_coordinate_start - step_y
                                #checken of na deze stap geen gate zit
                        elif coordinate in gate_coordinates and coordinate != coordinate_end:
                            x_coordinate_start = x_coordinate_start - step_x - step_x
                            # z kan nu niet meerdere stappen omhoog/omlaag
                            coordinate = [x_coordinate_start, y_coordinate_start, z_coordinate_start]
                            if gate_connections:
                                for key in gate_connections:
                                    selected_wires = gate_connections[key]
                                    if coordinate in selected_wires or coordinate in gate_coordinates:
                                        if coordinate != coordinate_end:
                                            x_coordinate_start = x_coordinate_start + step_x
                                            # z kan nu niet meerdere stappen omhoog/omlaag
                                            y_coordinate_start = y_coordinate_start - step_y
                                            #checken of na deze stap geen gate zit
                                            break
                                            # moet ook uit while loop breken!
                            elif coordinate in gate_coordinates and coordinate != coordinate_end:
                                x_coordinate_start = x_coordinate_start + step_x
                                # z kan nu niet meerdere stappen omhoog/omlaag
                                y_coordinate_start = y_coordinate_start - step_y
                                #checken of na deze stap geen gate zit
                    elif coordinate in gate_coordinates and coordinate != coordinate_end:
                        y_coordinate_start = y_coordinate_start - step_y
                        # z kan nu niet meerdere stappen omhoog/omlaag
                        x_coordinate_start = x_coordinate_start + step_x
                        #checken of na deze stap geen gate zit
                        coordinate = [x_coordinate_start, y_coordinate_start, z_coordinate_start]
                        if gate_connections:
                            for key in gate_connections:
                                selected_wires = gate_connections[key]
                                if coordinate in selected_wires or coordinate in gate_coordinates:
                                    if coordinate != coordinate_end:
                                        x_coordinate_start = x_coordinate_start - step_x - step_x
                                        # z kan nu niet meerdere stappen omhoog/omlaag
                                        break
                                        # moet ook uit while loop breken!
                            coordinate = [x_coordinate_start, y_coordinate_start, z_coordinate_start]
                            if gate_connections:
                                for key in gate_connections:
                                    selected_wires = gate_connections[key]
                                    if coordinate in selected_wires or coordinate in gate_coordinates:
                                        if coordinate != coordinate_end:
                                            x_coordinate_start = x_coordinate_start + step_x
                                            # z kan nu niet meerdere stappen omhoog/omlaag
                                            y_coordinate_start = y_coordinate_start - step_y
                                            #checken of na deze stap geen gate zit
                                            break
                                            # moet ook uit while loop breken!
                            elif coordinate in gate_coordinates and coordinate != coordinate_end:
                                x_coordinate_start = x_coordinate_start + step_x
                                # z kan nu niet meerdere stappen omhoog/omlaag
                                y_coordinate_start = y_coordinate_start - step_y
                                #checken of na deze stap geen gate zit
                        elif coordinate in gate_coordinates and coordinate != coordinate_end:
                            x_coordinate_start = x_coordinate_start - step_x - step_x
                            # z kan nu niet meerdere stappen omhoog/omlaag
                            coordinate = [x_coordinate_start, y_coordinate_start, z_coordinate_start]
                            if gate_connections:
                                for key in gate_connections:
                                    selected_wires = gate_connections[key]
                                    if coordinate in selected_wires or coordinate in gate_coordinates:
                                        if coordinate != coordinate_end:
                                            x_coordinate_start = x_coordinate_start + step_x
                                            # z kan nu niet meerdere stappen omhoog/omlaag
                                            y_coordinate_start = y_coordinate_start - step_y
                                            #checken of na deze stap geen gate zit
                                            break
                                            # moet ook uit while loop breken!
                            elif coordinate in gate_coordinates and coordinate != coordinate_end:
                                x_coordinate_start = x_coordinate_start + step_x
                                # z kan nu niet meerdere stappen omhoog/omlaag
                                y_coordinate_start = y_coordinate_start - step_y
                                #checken of na deze stap geen gate zit
                coordinate = [x_coordinate_start, y_coordinate_start, z_coordinate_start]
                wires.append(coordinate)
                print(coordinate)
                wire = classs.Wire(coordinate, connected_gate)
                allwires.append(wire)
                
                
    if y_coordinate_start < y_coordinate_end:
        step_y = 1
    elif y_coordinate_start > y_coordinate_end:
        step_y = -1