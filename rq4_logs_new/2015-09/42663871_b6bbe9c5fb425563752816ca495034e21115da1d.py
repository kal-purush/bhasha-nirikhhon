import numpy as np

__author__ = 'Michal Kononenko'

equilibrium_magnetic_field = np.array([0, 0, 1])


class UnableToSolveSystemError(Exception):
    pass


class BlochSystem(object):
    """
    Contains all constants and problem parameters for the Bloch equations
    """
    def __init__(self, magnetic_field=None,
                 time_list=None,
                 initial_state=np.zeros([1, 3]),
                 gyromagnetic_ratio=None,
                 t1=np.inf, t2=np.inf
                 ):
        self.magnetic_field = magnetic_field
        self.time_list = time_list
        self.initial_state = initial_state
        self.gyromagnetic_ratio = gyromagnetic_ratio
        self.t1 = t1
        self.t2 = t2

    @property
    def is_solvable(self):
        conditions = [
            self.time_list is not None,
            self.gyromagnetic_ratio is not None,
            self.magnetic_field is not None
        ]
        return all(conditions)

    def get_bloch_matrix(self, time):
        if not self.is_solvable:
            raise UnableToSolveSystemError('Unable to solve system')

        if isinstance(time, list):
            time = np.array(time)

        if isinstance(time, np.ndarray):
            t1 = self.t1*np.ones(time.shape)
            t2 = self.t2*np.ones(time.shape)
        else:
            t1 = self.t1
            t2 = self.t2

        gamma = self.gyromagnetic_ratio
        [bx, by, bz] = self.magnetic_field(time)

        return np.array(
            [
                [-1/t2, gamma * bz, -gamma*by],
                [-gamma*bz, -1/t2, gamma*bx],
                [gamma*by, -gamma*bx, -1/t1]
            ]
        )

    def solve(self):
        if not self.is_solvable:
            raise UnableToSolveSystemError('Cannot solve system')

        delta_t = self.time_list[1] - self.time_list[0]

        solution = np.zeros([len(self.initial_state), len(self.time_list)])

        solution[:, 0] = self.initial_state

        for index in xrange(0, len(self.time_list) - 1):
            time = self.time_list[index]
            bloch_matrix = self.get_bloch_matrix(time)
            eq_field = self.get_equilibrium_field()

            init = solution[:, index]

            solution[:, index+1] = init + delta_t * (np.dot(init, bloch_matrix) + eq_field)

        return solution

    def get_equilibrium_field(self):
        return np.array([0, 0, 1e-3/self.t1])


class MagneticField(object):
    """
    Models the magnetization vector :math:`\nathbb{B} = (B_x, B_y, B_z)`
    perturbing the system for which a solution is to be found

    :arg perturbation_functions: A list of three functions of x, y, and z,
    that give the value of the magnetic field in each spatial dimension. Each
    function must take in a floating-point time value as an argument and return
    the strength of the magnetic field in Tesla. Defaults to

    ..math::
        \mathbb{B} = ( \cos(t), \sin(t), 1)
    """
    def __init__(self, field_functions=None):
        """
        Instantiates the variables listed above
        """
        if field_functions is None:
            self.field_functions = [
                lambda t: np.cos(t), lambda t:np.sin(t), lambda t: 1
            ]
        else:
            self.field_functions = field_functions

    def __call__(self, time):
        """ Evaluates the magnetic field at a given time t
        :param float time: The time value for which the field is to be
        evaluated
        :return: A list of the magnetic field components
        """
        return [f(time) for f in self.field_functions]

    def __repr__(self):
        return '%s(%s)' % (self.__class__.__name__,
                           self.field_functions)


class SignalAnalyzer(object):
    """ Contains methods to analyze the return signal given a field and a
    Bloch system
    """
    def __init__(self, bloch, field):
        self.bloch_system = bloch
        self.field = field

    @property
    def driving_field(self):
        return self.field.field_functions[0:2]

    @driving_field.setter
    def driving_field(self, field_functions):
        funcs = self.field.field_functions
        funcs[0] = field_functions[0]
        funcs[1] = field_functions[1]
        self.field.field_functions = funcs
