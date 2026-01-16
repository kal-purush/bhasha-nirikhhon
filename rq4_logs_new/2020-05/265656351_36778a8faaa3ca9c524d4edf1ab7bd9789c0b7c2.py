import matplotlib.pyplot as plt
from mpl_toolkits.mplot3d import Axes3D
from pyquil.wavefunction import get_bitstring_from_index, Wavefunction
from qutip import Bloch, basis

def plot_bloch(wf: Wavefunction, axes=None, fig=None):
    if len(wf.amplitudes) > 2:
        raise ValueError('Bloch sphere plotting only works with 1Q')
    state = (wf.amplitudes[0] * basis(2,0) + wf.amplitudes[1] * basis(2,1))
    b = Bloch(fig=fig, axes=axes)
    b.add_states(state)
    b.render(fig=fig, axes=axes)

def plot_probabilities(wf: Wavefunction, axes=None, qubit_subset=None):
    prob_dict = wf.get_outcome_probs()
    if qubit_subset:
        sub_dict = {}
        qubit_num = len(wf)
        for i in qubit_subset:
            if i > (2**qubit_num - 1):
                raise IndexError("Index {} too large for {} qubits.".format(i, qubit_num))
            else:
                sub_dict[get_bitstring_from_index(i, qubit_num)] = prob_dict[get_bitstring_from_index(i, qubit_num)]
        prob_dict = sub_dict
    axes.set_ylim(0, 1)
    axes.set_ylabel('Outcome probability', fontsize=16)
    axes.set_xlabel('Bitstring outcome', fontsize=16)
    axes.bar(range(len(prob_dict)), prob_dict.values(), align='center', color='#6CAFB7')
    axes.set_xticks(range(len(prob_dict)))
    axes.set_xticklabels(prob_dict.keys(), fontsize=14)
    
def plot_wf(wf: Wavefunction, wf0=None, wf1=None, bitstring_subset=None):
    if len(wf.amplitudes) == 2:
        fig = plt.figure(figsize=(8, 6))
        wf_ax = fig.add_subplot(121)
        plot_probabilities(wf, axes=wf_ax, qubit_subset=bitstring_subset)
        bloch_ax = fig.add_subplot(122, projection='3d')
        plot_bloch(wf, axes=bloch_ax, fig=fig)
        fig.suptitle(f'$|\psi>$ = {wf}\n', fontsize=16)
    elif len(wf.amplitudes) == 4 and wf0 is not None and wf1 is not None:
        fig = plt.figure(figsize=(8, 6))
        wf_ax = fig.add_subplot(131)
        plot_probabilities(wf, axes=wf_ax, qubit_subset=bitstring_subset)
        bloch1_ax = fig.add_subplot(132, projection='3d')
        plot_bloch(wf1, axes=bloch1_ax, fig=fig)
        bloch0_ax = fig.add_subplot(133, projection='3d')
        plot_bloch(wf0, axes=bloch0_ax, fig=fig)
        fig.suptitle(f'$|\psi>$ = {wf}\n', fontsize=18)
    else:
        fig = plt.figure(figsize=(6, 6))
        wf_ax = fig.add_subplot(111)
        plot_probabilities(wf, axes=wf_ax, qubit_subset=bitstring_subset)
        fig.suptitle(f'$|\psi>$ = {wf}\n', fontsize=16)