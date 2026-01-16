from bisect import bisect_left
from collections import defaultdict
from functools import lru_cache
import itertools
import numpy as np
from collections import deque



#During recursion, for each gate, need to know:
# 1. Current grid gate is in
# 2. current location of gate
# 3. pads connected to gate
# 4. 


#During each recursion step, need to build: 
# 1. New adjacency matrix
# 2. New b matrixes
# 3. Need to be able to transfer this data
# 4. Need to know what gates are in grid cell

class QpSolver():
    def __init__(self) -> None:
        self._netdict = defaultdict(list)
        self.num_gates = None
        self.num_nets = None
        self.adjacency_matrix = None
        self._A_matrix = {}
        self._pad_net_dict = {}
        self._gate_pad_dict = defaultdict(list)
        self._pad_locations = {}
        self._net_gate_dict = defaultdict(list)
        self._num_grid_rows = 1
        self._num_grid_cols = 1
        self._grid_length = 0
        self._grid_heigth = 0
        self._gate_locations = {}
        self._gate_grid_span = {}
        self._EPSILON = 0.00000001
    

    def read_netlist(self, netlist_file_name):
        with open(netlist_file_name, 'r') as f:
            line1 = next(f)
            num_gates, num_nets = [int(val) for val in line1.strip().split()]
            self.num_gates = num_gates
            self.num_nets = num_nets
            
            for _ in range (num_gates):

                this_line = next(f)
                gate_id, num_nets_connected, *connected_nets = [int(val) for val in this_line.strip.split()]

                for net in connected_nets:
                    self._net_gate_dict[net].append(gate_id)
            
            next_line = next(f)
            num_pads = int(next_line.strip().split())
            
            for _ in range(num_pads):
                this_line = next(f)
                pad_id, net_connected, x_coord, y_coord = [int(val) for val in this_line.strip().split()]

                self._pad_locations[pad_id] = (x_coord, y_coord)
                for gate in self._netdict[net_connected]:
                    self._gate_pad_dict[gate].append(pad_id)


    def _net_dict_to_adjacency_matrix(self):
        self._adjacency_matrix = np.zeros((self.num_gates, self.num_gates), dtype=np.int32)

        for net_id, gate_list in self._netdict:
            for gate1, gate2 in itertools.combinations(gate_list):
                self.adjacency_matrix[gate1][gate2] = 1
                self.adjacency_matrix[gate2][gate1] = 1


    def _build_A_matrix(self):

        self._A_matrix  =  -self.adjacency_matrix.astype(np.float32)
        for gate_id in range(1, self.num_gates+1):
            row_sum = sum(self.adjacency_matrix[gate_id - 1])
            diag_term = row_sum + len(self._gate_pad_dict[gate_id])
            self._A_matrix[gate_id][gate_id] = diag_term
    

    def _build_b_matrixes(self):
        self._bx_matrix = np.zeros((self.num_gates, 1), dtype = np.int32)
        self._by_matrix = np.zeros((self.num_gates, 1), dtype = np.int32)

        for gate_id in range(1, self.num_gates+1):
            self._bx_matrix[gate_id-1] = self._gate_pad_dict[gate_id][0]
            self._by_matrix[gate_id-1] = self._gate_pad_dict[gate_id][1]

    
    def _build_b_matrix(self):
        pass
    


    def _calculate_grid_boundaries(self, grid_location):
        r, c = grid_location

        x_min = r * self._grid_length / self._num_grid_cols
        x_max = (r+1) * (self._grid_length / self._num_grid_cols)

        y_min = c * self._grid_height / self._num_grid_rows
        y_max = (c + 1) * self.grid_height/ self._num_grid_rows

        return (x_min, x_max, y_min, y_max)


    def _is_connected(self, gate1, gate2):
        return True if self.adjacency_matrix[gate1][gate2] == 1 else False
    
    def _in_same_cell(self, gate1, gate2):
        return True if self._grid_location[gate1] == self._grid_location[gate2]

    
    @lru_cache
    def _is_connected(self, pad, gate):
        return True if pad in self._gate_pad_dict[gate] else False

    def _propagate_pads(self, grid_location, grid_gates):
        x_min, x_max, y_min, y_max = self._calculate_grid_boundaries(grid_location)

        for gate_id in range(1, self.num_gates+1):
            select = 1 << (gate_id-1)

            if select and grid_gates == select:
                for pad in self._gate_pad_dict[gate_id]:
                    x_pad, y_pad = pad

                    if x_pad < x_min: x_pad = x_min
                    elif x_pad > x_max: x_pad = x_max

                    if y_pad < y_min: y_pad = y_min
                    elif y_pad > y_min: y_pad = y_max
                    

    def _propagate_pseudo_pads(self, grid_location, grid_gates)




    def _calculate_b_matrixes(self, span_gates, pseudo_pad_dict):

        n = len(span_gates)

        bx = np.zeros((n, 1))
        by = np.zeros((n, 1))

        for idx, gate in enumerate(span_gates):
            pads = pseudo_pad_dict[gate]

            num_pads = len(pads)
            bx[idx] = sum(pad[0] for pad in pads)
            by[idx] = sum(pad[1] for pad in pads)

        return bx, by

    
    def _propagate_pad_location(self, boundary, pad_location):
        
        start_x, end_x, start_y, end_y = boundary
        pad_x, pad_y = pad_location

        if pad_x < start_x: 
            pad_x = start_x
        else:
            pad_x = end_x - self._EPSILON
        
        if pad_y < start_y:
            pad_y = start_y
        else:
            pad_y = end_y - self._EPSILON

        return (pad_x, pad_y)


    def _build_pad_pseudo_pad_dict(self, boundary, span_gates, outside_gates):

        start_x, end_x, start_y, end_y = boundary

        pad_dict = defaultdict(list)

        for gate1 in span_gates:
            for gate2 in outside_gates:
                if self._adjacency_matrix[gate1][gate2] == 1:
                    gate_location = self._gate_locations[gate2]
                    propagated_location = self._propagate_pad_location(gate_location)
                    pad_dict[gate1].append(propagated_location)
        
        for gate in span_gates: 
            for pad_id in self._gate_pad_dict[gate]:
                pad_location = self._pad_locations[pad_id]
                propagated_location = self._propagate_pad_location(boundary, pad_location)
                pad_dict[gate].append(propagated_location)
        
        return pad_dict


    def _build_adjacency_matrix(self, span_gates):
        n = len(span_gates)
        C = np.zeros((n,n))

        idxs = [n for n in range(n)]
        for gate1_idx, gate2_idx in itertools.combinations(idxs):

            gate1 = span_gates[gate1_idx]
            gate2 = span_gates[gate2_idx]

            if self._adjacency_matrix[gate1][gate2] == 1: 
                C[gate1_idx][gate2_idx] = 1
                C[gate2_idx][gate1_idx] = 1 
        
        return C

    
    def _calculate_A_matrix(self, span_gates, pad_dict):
        C = self._build_adjacency_matrix(span_gates)

        A = -C

        for idx, gate in enumerate(span_gates):
            num_pads = len(pad_dict[gate])
            A[idx][idx] = num_pads
    
        return A


    def _build_grid_matrixes(self, span_gates, outside_gates):
        pad_dict = self._build_pad_pseudo_pad_dict(span_gates, outside_gates)
        A = self._calculate_A_matrix(span_gates, pad_dict)
        bx, by = self._calculate_b_matrixes(span_gates, pad_dict)

        return A, bx, by


    def repartition(self, gridspan):
        row_start, row_end, col_start, col_end = gridspan
        span_gates, outside_gates = self._find_gates_in_span(row_start, row_end, col_start, col_end)

        X, bx, by = self._build_grid_matrixes(span_gates, outside_gates)

        X = np.linalg.solve(X, bx)
        Y = np.linalg.solve(X, by)

        a = zip(span_gates, X, Y)
        mid = len(span_gates)//2

        if col_start-col_end >= row_start-row_end:
            a.sort(lambda x: (x[0], x[1]))
            split_col = (col_start + col_end)//2

            for gate, x_coord, y_coord in a[:mid+1]:
                self._gate_grid_span[gate] = (row_start, row_end, col_start, split_col)

            for gate, x_coord, y_coord in a[mid+1:]:
                self._gate_grid_span[gate] = (row_start, row_end, split_col, col_end)
        
        else:
            a.sort(lambda x: (x[1], x[0]))
            split_row = (row_start + row_end) // 2

            for gate, x_coord, y_coord in a[:mid+1]:
                self._gate_grid_span[gate] = (row_start, split_row, col_start, col_end)
            
            for gate, x_coord, y_coord in a[:mid+1]:
                self._gate_grid_span[gate] = (split_row, row_end, col_start, col_end)

        
        for gate, x_coord, y_coord in a: 
            self._gate_locations[gate] = (x_coord, y_coord)




    def _span_split(grid_span):
        
        row_start, row_end, col_start, col_end = grid_span
        col_span = col_end - col_start
        row_span = row_end - row_start

        split = []

        if col_span == row_span == 0:
            split =  [grid_span]
        
        elif col_span >= row_span:
            col_split = (col_start + col_end)//2
            split.append((row_start, row_end, col_start, col_split))
            split.append((row_start, row_end, col_split, col_end))
        
        else:
            row_split = (row_start + row_end)//2
            split.append((row_start, row_split, col_start, col_end))
            split.append((row_split, row_end, col_start, col_end))
        
        return split
            

    def recursive_repartition(self, boundaries):

        row_start, row_end, col_start, col_end = boundaries


        recurse_stack = deque()
        recurse_stack.append(boundaries)

        while recurse_stack:
            this_boundary = recurse_stack.pop()
            splits = self._span_split(boundaries)
            if len(splits) == 2: 
                self.repartition(this_boundary)
                recurse_stack.extendleft(splits)

        #I'm going to assume span_gates is sorted for now. Really there isn't any reason it shouldn't
        #In any case, it should be a problem for self.find_gates_in_span






    def _find_gates_in_span(self, gridspan):

        row_start, row_end, col_start, col_end = gridspan

        span_gates = []
        outside_gates = []

        span = (row_start, row_end, col_start, col_end)

        #comment
            # row_start_y = self._grid_row_points[row_start]
            # row_end_y = self._grid_row_points[row_end + 1]

            # col_start_x = self._grid_col_points[col_start]
            # col_end_x = self._grid_col_points[col_end+1]

            # for gateid in range(1, self.num_gates+1):
            #     gate_x, gate_y = self._gate_locations[gateid]

            #     within_col = gate_x >= col_start_x and gate_x < col_end_x
            #     within_row = gate_y >= row_start_y and gate_y < row_end_y
                
            #     if within_col and within_row: 
            #         span_gates.append(gateid)
            #     else:
            #         outside_gates.append(gateid)
        
        for gateid in range(1, self.num_gates + 1):
            if self._gate_grid_span[gateid] == span:
                span_gates.append(gateid)
            else:
                outside_gates.append(gateid)

        return span_gates, outside_gates
          


    def recursive_partition(self, num_rows, num_cols):
        if not self._grid_cells or len(self.grid_cells[0]) != num_rows or len(self.grid_cells[1] != num_cols):
            self._grid_cells = [[0] * num_cols for _ in range(num_rows)]
        
        grid_row_gap = self._grid_heigth/num_rows
        grid_col_gap = self._grid_length/num_cols

        self._grid_row_points = [r * grid_row_gap for r in range(0, num_rows)]
        self._grid_col_points = [c * grid_col_gap for c in range(0, num_cols)]





    

