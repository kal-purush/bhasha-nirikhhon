from Simon import make_simon_circuit, make_oracle, post_processing
from tqdm import tqdm
import cirq
import time
from collections import Counter
import numpy as np
import os

def simon_random_test(number_of_qubits, runs_per_test, number_of_tests):
    quantum_res = []                                                                # Store the outout secret strings from Simon's algorithm
    ground_truth = []                                                               # Store the real secret strings, which are randomly picked.

    for i in tqdm(range(number_of_tests)):
        data = []                                                                   # Store results for MLE

        s = np.random.randint(low=0, high=1, size=number_of_qubits)                 # Generate random s, including all-zero string.
        ground_truth.append(''.join([str(x) for x in s]))

        for _ in range(runs_per_test):
            flag = False                                                            # Check linear independency of output
            while not flag:
                input_qubits = [cirq.GridQubit(i, 0) for i in range(number_of_qubits)]          # Define input qubits
                output_qubits = [                                                               # Define output qubits
                    cirq.GridQubit(i + number_of_qubits, 0) for i in range(number_of_qubits)
                ]

                oracle = make_oracle(input_qubits, output_qubits, s)                            # Create oracle for s, where s could be all-zero,
                                                                                                # representing a 1-1 function
                circuit = make_simon_circuit(input_qubits, output_qubits, oracle)               # Create Simon's circuit
                with open(path + '/simon_' + str(number_of_qubits) + 'bits.qasm', 'w') as f:    # Generate qasm files
                    f.write(circuit.to_qasm())
                simulator = cirq.Simulator()
                results = [                                                                     # Query the function n-1 times
                    simulator.run(circuit).measurements['result'][0] for _ in range(number_of_qubits - 1)
                ]

                flag = post_processing(data, results)                                           # Classical processing

        freqs = Counter(data)                                                                   # Afterwards, find the most likely result
        res, freq = sorted(freqs.items(), key=lambda x:x[1], reverse=True)[0]
        # Ideally, there should be only one tuple if the function is 2-1. But to make room for mistakes, we allow for more than one s.
        # But if the probability of this MLE is not high enough, then the answer must be all-zero string, namely the function might be 1-1.
        if int(freq) < 0.8 * runs_per_test:
            res = ''.join(['0'] * number_of_qubits)
        quantum_res.append(res)


    print("Our Simon's algorithm handled {} qubits problem, and every test ran at most {} times.".format(number_of_qubits, runs_per_test))
    try:
        assert quantum_res == ground_truth                                                      # Compare the list of results and groundtruth.
    except:
        cnt = 0
        for i in range(number_of_tests):
            if quantum_res[i] != ground_truth[i]: cnt += 1
        print("Simon solver solver failed {} tests out of {}.".format(cnt, number_of_tests))
        print("The ratio of failure is {}.".format(cnt/number_of_tests))
        return

    print("It passed {} random tests.".format(number_of_tests))
    return


# Define the director to save simon's qasm files
path = 'qasm/simon'
if not os.path.exists(path):
    os.mkdir(path)

# Custom settings on simon's random test
for i in range(2, 6):
    simon_random_test(number_of_qubits=i, runs_per_test=10, number_of_tests=1)