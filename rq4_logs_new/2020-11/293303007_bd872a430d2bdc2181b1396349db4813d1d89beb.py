import os
import csv
import time
from src.Utils import read_graph
from src.Graph import Graph
from src.CFGrammar import CFGrammar
from src.cfpq import *
from src.RFA import RFA

REPEAT_TIMES = 3

#DATASETS = ['FullGraph', 'MemoryAliases', 'SparseGraph', 'WorstCase']
DATASETS = ['FullGraph', 'MemoryAliases']
HEADER = ['Graph', 'Query', 'Converting to cnf', 'Converting to rfa from file'
          , 'hellings', 'matrices', 'tensor', 'tensor wcnf', 'tensor via rfa directly'
          , 'Number of pairs']

GRAPH_DIR = 'graphs/'
GRAMMAR_DIR = 'grammars/'


def calculate_time(start, end):
    return (end - start) / 1e+9


def init_result_file(path):
    with open(path, 'a+') as csvfile:
        csv_writer = csv.writer(csvfile)

        csv_writer.writerow(HEADER)


def run_benchmark():
    for data_set in DATASETS:
        directory = os.path.join(DATASET_PATH, data_set)
        res_path = os.path.join(directory, 'res.csv')
        init_result_file(res_path)

        graphs_directory = os.path.join(directory, GRAPH_DIR)
        grammars_directory = os.path.join(directory, GRAMMAR_DIR)

        graphs = os.listdir(graphs_directory)
        graphs.sort()

        for graph_file_name in graphs:
            edges = read_graph(os.path.join(graphs_directory, graph_file_name))
            graph = Graph(edges)

            grammars = os.listdir(grammars_directory)
            grammars.sort()

            for grammar_file_name in grammars:
                for _ in range(REPEAT_TIMES):
                    names = ['hellings', 'by matrices', 'tensor', 'tensor_wcnf', 'tensor_rfa']
                    algorithms = [cfpq_hellings, cfpq_matrices, cfpq_tensor, cfpq_tensor_wcnf, cfpq_tensor_with_rfa] 
                    times, results , nvals = dict(), dict(), dict()

                    cfg_wrapper = CFGrammar()

                    start = time.time_ns()
                    cfg_wrapper.cfg = CFGrammar.read_grammar_with_regex(os.path.join(grammars_directory, grammar_file_name))
                    cfg_wrapper.eps = cfg_wrapper.cfg.generate_epsilon()
                    cfg_wrapper.cnf = cfg_wrapper.cfg.to_normal_form()
                    end = time.time_ns()
                    grammar_cnf_time = calculate_time(start, end)

                    start = time.time_ns()
                    rfa = RFA.create_from_cfg_regex(os.path.join(grammars_directory, grammar_file_name))
                    end = time.time_ns()
                    rfa_time = calculate_time(start, end)

                    for name, cfg, algo in zip(names, [cfg_wrapper] * 5, algorithms):
                        start = time.time_ns()
                        if name == 'tensor_rfa':
                            results[name] = set(algo(graph, rfa))
                        else:
                            results[name] = set(algo(graph, cfg))
                        end = time.time_ns()
                        nvals[name] = len(results[name])
                        times[name] = calculate_time(start, end)
                        print(graph_file_name, grammar_file_name, name, nvals[name], times[name])

                    assert results['hellings'] == results['by matrices']
                    assert results['by matrices'] == results['tensor']
                    assert results['tensor'] == results['tensor_wcnf']
                    assert results['tensor_wcnf'] == results['tensor_rfa']
                    assert nvals['hellings'] == nvals['by matrices'] == nvals['tensor'] == nvals['tensor_wcnf'] == nvals['tensor_rfa']

                    with open(res_path, 'a+') as csvfile:
                        csv_writer = csv.writer(csvfile)

                        csv_writer.writerow([ graph_file_name, grammar_file_name
                                            , grammar_cnf_time, rfa_time
                                            , times['hellings'], times['by matrices'], times['tensor']
                                           , times['tensor_wcnf'], times['tensor_rfa'], nvals['hellings'] ])


if __name__ == '__main__':
    DATASET_PATH = os.path.join(os.getcwd(), 'benchmark_queries/DataForCFPQ/')

    run_benchmark()