#/usr/bin/env/python
# encoding: utf-8

'''
Exhaustively search for the ground state electron configuration of the given DB 
layout.
'''

__author__      = 'Samuel Ng'
__copyright_    = 'Apache License 2.0'
__version__     = '0.1'
__date__        = '2019-05-04'  # last update

from collections import namedtuple
import numpy as np
from scipy.spatial import distance
import argparse
from tqdm import tqdm
import siqadconn
import itertools
import multiprocessing as mp
import time

ElectronConfig = namedtuple('ElectronConfig', ['config', 'energy', 'validity'])

dp = 6
zero_diff = 10**(-dp)
equal = lambda a, b: abs(a - b) < zero_diff
less_than = lambda a, b: (b - a) > zero_diff
greater_than = lambda a, b: (a - b) > zero_diff
ffmt = lambda x: f'{x:.{dp}f}'


class ChargeConfig:
    '''
    Registers representing a charge configuration, can be 2 or 3 states.
    '''

    dbs = None      # list of DB locations in (x, y) angstrom coordinates
    v_ij = None     # precalculated V_ijs

    # constants
    q0 = 1.602e-19
    eps0 = 8.854e-12
    k_b = 8.617e-5

    def __init__(self, state_count, dbs, start_ind, end_ind, mu, epsilon_r, 
            debye_length, verbose):
        '''
        Args:
            state_count:    Number of states (2 for DB- and DB0, 3 to also 
                            include DB+).
            dbs:            List of DB locations in Euclidean coordinates in
                            angstrom.
            start_ind:      Offset from the beginning.
            end_ind:        Ending index (exclusive).
            mu:             Potential between Fermi level and (0/-) charge 
                            transition level.
            epsilon_r:      Relative permittivity.
            debye_length:   Thomas-Fermi Debye length.
        '''
        assert(start_ind >= 0 and start_ind < end_ind)

        self.state_max = 1 if state_count == 3 else 0   # max pos charge state (0 or 1)
        self.db_states = np.full(len(dbs), -1)          # -1, 0, 1 for DB-, DB0, and DB+
        if start_ind != 0:
            n = start_ind
            db_ind = len(dbs)-1
            while n:
                n, r = divmod(n, 3)
                if r == 1:
                    self.db_states[db_ind] = 0
                elif r == 2:
                    self.db_states[db_ind] = 1
                db_ind -= 1
        print(f'start state is {self.db_states}')

        # TODO add offset functionality probably through modulus
        self.curr_ind = start_ind
        self.end_ind = min(end_ind, 3**len(dbs))

        self.verbose = verbose

        if self.dbs == None:
            self.perform_precalculations(dbs, mu, epsilon_r, debye_length)

    def perform_precalculations(self, dbs, mu, epsilon_r, debye_length):
        '''
        Perform precalculations.
        '''

        self.dbs = np.asarray(dbs)
        
        # retrieve and process simulation parameters
        K_c = 1./(4 * np.pi * epsilon_r * self.eps0)

        # precompute distances and inter-DB potentials
        db_r = distance.cdist(self.dbs, self.dbs, 'euclidean')
        self.neighbor_rank = np.delete(np.argsort(db_r), 0, 1) # first column is always self index
        self.v_ij = np.divide(self.q0 * K_c * np.exp(-db_r/debye_length), 
                db_r, out=np.zeros_like(db_r), where=db_r!=0)
        if self.verbose:
            print('v_ij=\n{}'.format(self.v_ij))

        # local potentials
        self.mu = mu
        self.mus = np.ones(len(self.dbs)) * self.mu * -1
        self.eta = 0.59                         # TODO make configurable
        self.v_ext = np.zeros(len(self.dbs))    # TODO add support for ext potential
        self.v_i = np.full(len(self.dbs), -float('inf'))    # Overwritten over each advance
        self.v_i_ready = False

    def advance(self):
        '''
        Advance the configuration into the next state.

        Returns:
            A bool informing the success state of the advancement. False when 
            the config has reached the end.
        '''

        self.curr_ind += 1
        if self.curr_ind >= self.end_ind:
            return False

        charge_ind = len(self.db_states) - 1
        carry = 1
        while charge_ind >= 0 and carry > 0:
            if self.db_states[charge_ind] != self.state_max:
                # increment charge
                carry = 0
                self.db_states[charge_ind] += 1
            else:
                # reset charge and add carry
                carry = 1
                self.db_states[charge_ind] = -1
            charge_ind -= 1

        self.v_i[:] = -float('inf')
        self.v_i_ready = False

        return True

    def has_next(self):
        '''
        Check whether there is a next state without actually advancing there.

        Returns:
            A bool indicating whether there is a next state.
        '''
        return self.curr_ind + 1 < self.end_ind

    def system_energy(self, use_qubo=False):
        '''
        Calculate and return the system energy either in paper form or in QUBO
        form.

        Args:
            use_qubo:   Use the QUBO formulation if set to true.

        Returns:
            The system energy in paper form or in QUBO form in float.
        '''

        # TODO implement QUBO formulation
        
        if not self.v_i_ready:
            for i in range(len(self.dbs)):
                if self.v_i[i] == -float('inf'):
                    self._calc_v_i(i)
            self.v_i_ready = True
        return np.inner(self.db_states, self.v_i)

    def physically_valid(self):
        '''
        Determine the physical validity of the current config. This includes 
        checking the charge config stability and population stability.

        Returns:
            A bool indicating whether this config is physically valid.
        '''

        # population stability and calculate v_i as needed
        for i in range(len(self.dbs)):
            self._calc_v_i(i)
            valid = (self.db_states[i] == 1  and less_than(self.v_i[i] + self.mu + self.eta, 0)) or \
                    (self.db_states[i] == -1 and greater_than(self.v_i[i] + self.mu, 0)) or \
                    (self.db_states[i] == 0  and less_than(self.v_i[i] + self.mu, 0) \
                                             and greater_than(self.v_i[i] + self.mu + self.eta, 0))
            if not valid:
                if self.verbose:
                    print(f'Config {self.db_states} population unstable, failed at '
                            f'index {i} with v_i={self.v_i[i]}, mu={self.mu}, '
                            f'eta={self.eta}')
                return False

        self.v_i_ready = True

        # configuration stability
        for i in range(len(self.dbs)):
            # Do nothing with DB+
            if self.db_states[i] == 1:
                continue

            # Attempt hops from more negative charge states to more positive ones
            for j in range(len(self.dbs)):
                if (self.db_states[j] > self.db_states[i]) \
                        and (less_than(self._hop_energy_delta(i, j), 0)):
                    if self.verbose:
                        print(f'Config {self.db_states} charge state '
                                f'unstable, failed when hopping from site '
                                f'{i} to {j}')
                    return False
        return True

    def _calc_v_i(self, ind):
        '''
        Calculate the V_i of the given ind and store it in self.v_i[ind].
        '''
        self.v_i[ind] = self.v_ext[ind] + np.dot(self.v_ij[ind][:], self.db_states)

    def _hop_energy_delta(self, i, j):
        '''
        Calculate the energy delta from hopping DB at site i to j.

        Returns:
            The energy delta as a float.
        '''
        return self.v_i[i] - self.v_i[j] - self.v_ij[i][j]


class ExhaustiveGroundStateSearch:
    '''Exhaustively find the ground state configuration of the given DB layout.
    '''

    elec_configs = []

    lat_a = 3.84    # lattice vector in x, angstroms    (intra dimer row)
    lat_b = 7.68    # lattice vector in y, angstroms    (inter dimer row)
    lat_c = 2.25    # dimer pair separation, angstroms

    def __init__(self, in_file, out_file, verbose=False):
        self.in_file = in_file
        self.out_file = out_file
        self.verbose = verbose

        self.sqconn = siqadconn.SiQADConnector('Exhaustive Ground State Searcher',
                self.in_file.name, self.out_file)

    def ground_state_search_3_states(self, num_threads, stability_checks='all',
            include_states='ground', use_qubo_obj_func=False):
        '''
        Search for the ground state using multiple threads.

        Args:
            num_threads:        Number of threads to spawn.
            stability_checks:   Options 'population_only' or 'all'.
            include_states:     Options 'ground', 'valid' or 'all'.
            use_qubo_obj_func:  Set to true to use QUBO objective function as 
                                the energy output.
        '''

        db_scale = 1e-10    # ang to m
        sq_param = lambda key : self.sqconn.getParameter(key)
        lat_coord_to_eucl = lambda n, m, l: (db_scale * n * self.lat_a, db_scale * (m * self.lat_b + l * self.lat_c))

        manager = mp.Manager()
        managed_elec_configs = manager.list([])
        managed_cpu_time_list = manager.list([])

        # retrieve sim info
        dbs = np.asarray([lat_coord_to_eucl(db.n, db.m, db.l) \
                for db in self.sqconn.dbCollection()])
        epsilon_r = float(sq_param('epsilon_r'))
        debye_length = float(sq_param('debye_length')) * 1e-9
        mu = float(sq_param('global_v0'))
        v_ext = np.zeros(len(dbs)) # TODO implement

        # prepare threads
        max_config_id = 3**len(dbs)
        if num_threads == None:
            num_threads = int(sq_param('num_threads'))
        if num_threads <= 0 or num_threads > max_config_id:
            num_threads = min(mp.cpu_count(), max_config_id)
        configs_per_thread = int(np.ceil(max_config_id / num_threads))
        curr_range = (0, configs_per_thread)
        threads = []
        processes = []
        thread_id = 0
        while curr_range[1] <= max_config_id and curr_range[0] != curr_range[1]:
            th = SearchThreadThreeStates(managed_elec_configs, managed_cpu_time_list, 
                    thread_id, curr_range, dbs, mu, epsilon_r, debye_length,
                    use_qubo_obj_func, self.verbose)
            p = mp.Process(target=th.run)
            threads.append(th)
            processes.append(p)
            curr_range = curr_range[1], min(curr_range[1]+configs_per_thread, max_config_id)
            thread_id += 1

        wall_time_start = time.time()

        [p.start() for p in processes]
        [p.join() for p in processes]

        self.wall_time_elapsed = time.time() - wall_time_start

        # find the actual ground states among the returned states
        gs_energy = float('inf')
        for elec_config in managed_elec_configs:
            if less_than(elec_config.energy, gs_energy):
                self.elec_configs.clear()
                self.elec_configs.append(elec_config)
                gs_energy = elec_config.energy
            elif equal(elec_config.energy, gs_energy):
                self.elec_configs.append(elec_config)

        self.cpu_time = np.sum(managed_cpu_time_list)

    def export_results(self, export_json=False):
        '''Export the results sorted by the energy given by the objective
        function.'''
        import json

        if export_json:
            with open('result.json', 'w') as outfile:
                #json.dump(sorted(self.elec_configs, key=lambda config: config.energy), outfile)
                json.dump(self.elec_configs, outfile)

        # DB locations
        dblocs = []
        for db in self.sqconn.dbCollection():
            dblocs.append((str(db.x), str(db.y)))
        self.sqconn.export(db_loc=dblocs)

        # charge configurations
        def config_to_str(config):
            config_str = ''
            for charge in config:
                if charge == 1:
                    config_str += '+'
                elif charge == 0:
                    config_str += '0'
                elif charge == -1:
                    config_str += '-'
                else:
                    raise ValueError(f'Unknown charge value: {charge}')
            return config_str
        charge_configs = []
        for elec_config in self.elec_configs:
            charge_configs.append([config_to_str(elec_config.config), 
                ffmt(elec_config.energy), str(1), str(int(elec_config.validity))])
        self.sqconn.export(db_charge=charge_configs)

        # timing information
        self.sqconn.export(misc=[['time_s_cpu_time', self.cpu_time],
                                 ['time_s_wall_time', self.wall_time_elapsed]])


class SearchThreadThreeStates:
    '''A single search thread.'''

    def __init__(self, managed_config_results, managed_time_list, t_id, 
            search_range, dbs, mu, epsilon_r, debye_length, use_qubo_obj_func, 
            verbose):
        '''search_range is a tuple containing the start and end indices.'''
        self.managed_config_results = managed_config_results
        self.managed_time_list = managed_time_list
        self.thread_id = t_id
        self.dbs = dbs
        self.verbose = verbose
        self.use_qubo_obj_func = use_qubo_obj_func

        self.config = ChargeConfig(3, self.dbs, search_range[0],
                search_range[1], mu, epsilon_r, debye_length, verbose)

    def run(self):
        time_start = time.process_time()
        all_configs = []
        gs_configs = []
        gs_energy = float('inf')

        has_next = self.config.has_next()
        while has_next:
            valid = self.config.physically_valid()
            if valid:
                energy = self.config.system_energy()
                if (valid and less_than(energy, gs_energy)):
                    gs_configs.clear()
                    gs_configs.append(self.config.db_states.copy())
                    gs_energy = energy
                elif (valid and equal(energy, gs_energy)):
                    gs_configs.append(self.config.db_states.copy())

            has_next = self.config.advance()
        print(f'Last state: {self.config.db_states}')

        print(f'Found ground states: {gs_configs}')

        elec_configs = []
        for gs_config in gs_configs:
            elec_configs.append(ElectronConfig(gs_config, gs_energy, 1))
        self.managed_config_results.extend(elec_configs)
        self.managed_time_list.append(time.process_time()-time_start)


def parse_cml_args():
    '''Parse command-line arguments.'''

    parser = argparse.ArgumentParser(description='Exhaustively find the ground '
            'state configuration of the given DB layout.')
    parser.add_argument(dest='in_file', type=argparse.FileType('r'), 
            help='Path to the simulation problem file.',
            metavar='IN_FILE')
    parser.add_argument(dest='out_file', help='Path to the '
            'result file.', metavar='OUT_FILE')
    parser.add_argument('--num-threads', dest='num_threads', type=int, 
            help='Number of threads to run concurrently, leave blank to use all '
            'threads available.')
    parser.add_argument('--stability-checks', dest='stability_checks',
            default='all', const='all', nargs='?', 
            choices=['population_only', 'all'],
            help='Indicate which stability checks to perform.')
    parser.add_argument('--include-states', dest='include_states', default='ground',
            const='ground', nargs='?', choices=['ground', 'valid', 'all'],
            help='Indicate which states to include - ground for only the ground '
            'state, valid for all the valid states, all for everything.')
    parser.add_argument('--use-qubo-obj-func', action='store_true',
            dest='use_qubo_obj_func')
    parser.add_argument('--export-json', action='store_true', dest='export_json')
    parser.add_argument('--verbose', action='store_true', dest='verbose')
    return parser.parse_args()

if __name__ == '__main__':
    cml_args = parse_cml_args()
    print('Setting up problem...')
    egss = ExhaustiveGroundStateSearch(cml_args.in_file, cml_args.out_file, 
            verbose=cml_args.verbose)
    print('Performing exhaustive search...')
    egss.ground_state_search_3_states(cml_args.num_threads, cml_args.stability_checks,
            cml_args.include_states, cml_args.use_qubo_obj_func)
    print('Exporting results...')
    egss.export_results(cml_args.export_json)
    print('Finished')