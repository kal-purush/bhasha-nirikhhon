######################################################################
# TorchANI interface: https://github.com/aiqm/torchani
#
# Copyright: 2022
#
# Authors: Lester Hedges   <lester.hedges@gmail.com>
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 2 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program If not, see <http://www.gnu.org/licenses/>.
######################################################################

import numpy as np

import torch
import torchani

import ash.modules.module_coords

from ash.modules.module_coords import elematomnumbers

from ash.functions.functions_general import (
    ashexit,
    BC
)

class TorchANITheory:

    # Follow ASH style for constructor, where required arguments are keywords,
    # rather than positional arguments.
    def __init__(self, fragment=None, qmatoms=None, printlevel=2, numcores=1):
        """Constructor.

           Parameters
           ----------

           fragment : ash.Fragment
               The ASH fragment object.

           qmatoms : [int]
               Indices of atoms in the QM region.

           printlevel : int
               Verbosity level.
        """

        # Validate input.

        if not isinstance(fragment, ash.modules.module_coords.Fragment):
            raise TypeError("'fragment' must be of type 'ash.modules.module_coords.Fragment'.")
        self._fragment = fragment

        if not isinstance(qmatoms, (list, tuple, np.ndarray)):
            raise TypeError("'qmatoms' must be of type 'list', 'tuple', or 'numpy.ndarray'.")
        if not all(isinstance(x, int) for x in qmatoms):
            raise TypeError("'qmatoms' can only contain items of type 'int'.")
        num_atoms = self._fragment.numatoms
        for qmatom in qmatoms:
            if qmatom < 0 or qmatom >= num_atoms:
                raise ValueError(f"'qmatoms' index {qmatom} outside range of "
                                 f"fragment with {num_atoms} atoms.")
        self._qmatoms = qmatoms

        if type(printlevel) is not int:
            raise TypeError("'printlevel' must be of type 'int'.")
        self._printlevel = printlevel

        # Work out the atomic numbers for the elements in the QM region.
        # Because ASH pefroms zero self-consistency checks, these could be
        # different to those of the qm_elems when the run method is called.
        # We assume that no-one is stupid enough to do this.
        atomic_numbers = []
        for atom in self._qmatoms:
            # Get the element as a lower case string.
            elem = self._fragment.elems[atom].lower()

            # Store the atominic number.
            atomic_numbers.append(elematomnumbers[elem])

        # Create the device. Use CUDA as the default, falling back on CPU.
        self._device = torch.device("cuda" if torch.cuda.is_available() else "cpu")

        # Create the model.
        self._model = torchani.models.ANI2x(periodic_table_index=True).to(self._device)

        # Convert the atomic numbers to a Torch tensor.
        self._qm_atomic_numbers = torch.tensor([atomic_numbers], device=self._device)

        # Set the number of cores. Needed to be used as a QMTheory, but
        # completely redundant.
        self.numcores = 1

    # Match run function of other interface objects.
    def run(self, current_coords=None, charge=None, mult=None,
            current_MM_coords=None, MMcharges=None, qm_elems=None,
            Grad=False, numcores=None, label=None):
        """Calculate the energy and (optionally) gradients.

           Parameters
           ----------

           current_coords : numpy.ndarray
               The current QM coordinates.

           charge : int
               Charge of the QM region.

           mult : int
               Spin multiplicity of the QM region.

           current_MM_coords : numpy.ndarray
               The MM point-charge coordinates.

           MMcharges : [ float ]
               The MM point-charge charges.

           qm_elems : [ str ]
               A list of elements for the QM region.

           Grad : bool
               Whether to compute gradients.

           numcores : int
               The number of cores to use for the QM backend.

           label : str
               Job identification string.
        """

        if self._printlevel >= 2:
            print(BC.OKBLUE,BC.BOLD, "------------RUNNING TORCHANI INTERFACE-------------", BC.END)

        # Validate the input. Annoyingly, the user could call the run method
        # using input that is inconsistent with that used to instatiate the
        # MLMM object, i.e. coordinates corresponding to a different fragment,
        # or QM region. This is a general issue with ASH, though, and there
        # is no type checking or data validation elsewhere in the code. Here
        # we assume that the data is consistent and simply type check the input.

        if current_MM_coords is not None:
            raise TypeError("'current_MM_coords' is not 'None'. TorchANITheory "
                            "only supports calculations for the QM region only.")

        if MMcharges is not None:
            raise TypeError("'MMcharges' is not 'None'. TorchANITheory doesn't support "
                            "electrostatic embedding!")

        if not isinstance(current_coords, np.ndarray):
            raise TypeError("'current_coords' must be of type 'numpy.ndarray'.")
        if current_coords.dtype != np.float64:
            raise TypeError("'current_coords' must have dtype 'float64'.")

        if not isinstance(qm_elems, list):
            raise TypeError("'qm_elems' must be of type 'list'.")
        if not all(isinstance(x, str) for x in qm_elems):
            raise TypeError("'qm_elems' must be a list of 'str' types.")

        if not isinstance(Grad, bool):
            raise TypeError("'Grad' must be of type 'bool'.")

        if label:
            if not isinstance(label, str):
                raise TypeError("'label' must be of type 'str'.")

        # Convert the coordinates to a Torch tensor, casting to 32-bit floats.
        coords = torch.tensor([np.float32(current_coords)], requires_grad=Grad, device=self._device)

        # Compute the energy.
        energy = self._model((self._qm_atomic_numbers, coords)).energies

        if not Grad:
            return energy.detach().numpy()[0]

        # Optionally, compute the gradients too.
        else:
            gradient = torch.autograd.grad(energy.sum(), coords)[0]

            return energy.detach().numpy()[0], gradient.numpy()[0]