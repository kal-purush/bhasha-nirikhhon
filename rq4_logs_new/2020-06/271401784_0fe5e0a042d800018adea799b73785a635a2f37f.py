import numpy as np
from chainer.backends.cuda import cupy as cp
from chainer import FunctionNode, backend, Variable
from chainer.backends import cuda
import chainer.functions as F
import chainer.links as L


class StraightThroughEstimator(FunctionNode):
    def forward_cpu(self, inputs):
        # Unpack inputs
        a, z = inputs

        # Prepare output variable
        out = a.copy()
        out[z > 1] = 0
        out[z < -1] = 0

        return out,

    def forward_gpu(self, inputs):
        # Unpack inputs
        a, z = inputs

        # Prepare kernel
        ste = cuda.elementwise(
            'float32 x, float32 y',
            'float32 z',
            'if (y >= 1.f || y <= -1.f)'
            '{'
            'z = 0.f;'
            '}'
            'else'
            '{'
            'z = x;'
            '}',
            'straight_through_estimator'
        )

        out = ste(a, z)
        return out,


class DeterministicBinarization(FunctionNode):
    def forward_cpu(self, inputs):
        # Unpack inputs
        a, = inputs

        # Remember Inputs for ste
        self.retain_inputs((0, ))

        # Compute Output
        o = np.ones_like(a)
        o = np.copysign(o, a)
        return o,

    def forward_gpu(self, inputs):
        # Unpack inputs
        a, = inputs

        # Remember Inputs for ste
        self.retain_inputs((0, ))

        # Compute Output
        o = cp.ones_like(a)
        o = cp.copysign(o, a)
        return o,

    def backward(self, target_input_indexes, grad_outputs):
        # Unpack inputs.
        a, = self.get_retained_inputs()
        ga, = grad_outputs

        # Compute StraightThroughEstimator
        return StraightThroughEstimator().apply((ga, a))


class StochasticBinarization(FunctionNode):
    def forward_cpu(self, inputs):
        # Unpack inputs
        a, = inputs

        # Remember Inputs for ste
        self.retain_inputs((0, ))

        # Compute Output
        sigmoid_a = np.clip((a + 1) / 2, 0, 1)
        random = np.random.random_sample(a.shape)

        # Compute Output
        o = np.where(random <= sigmoid_a, 1., -1.)
        return o,

    def forward_gpu(self, inputs):
        # Unpack inputs
        a, = inputs

        # Remember Inputs for ste
        self.retain_inputs((0,))

        # Compute Output
        sigmoid_a = cp.clip((a + 1)/2, 0, 1)
        random = cp.random.random_sample(a.shape)

        o = cp.where(random <= sigmoid_a, 1., -1.).astype(dtype=cp.float32, copy=False)
        return o,

    def backward(self, target_input_indexes, grad_outputs):
        # Unpack inputs.
        a, = self.get_retained_inputs()
        ga, = grad_outputs

        # Compute StraightThroughEstimator
        return StraightThroughEstimator().apply((ga, a))


class ResidualBinarization(FunctionNode):

    def __init__(self):
        self.residual_errors = []

    def forward(self, inputs):
        # Unpack inputs
        x, levels = inputs

        # Retain input for backward step
        self.retain_inputs((0, 1))

        # Get module
        xp = backend.get_array_module(*inputs)

        # Compute output
        self.residual_errors.append(x)
        e = 0
        for l in range(levels.shape[0]):
            r_b = deterministic_binarization(self.residual_errors[l]).array
            e = e + r_b * levels[l]
            if (l+1) < levels.shape[0]:
                self.residual_errors.append(self.residual_errors[l] - (r_b * levels[l]))

        return e,

    def backward(self, target_input_indexes, grad_outputs):
        # Unpack inputs.
        a, levels = self.get_retained_inputs()
        ga, = grad_outputs

        xp = backend.get_array_module(a, levels, ga)

        residual_binary_errors = [deterministic_binarization(error).array for error in self.residual_errors]

        glevels = xp.zeros_like(levels.array)
        for l in range(levels.shape[0]):
            glevels[l] = xp.sum(ga.array * residual_binary_errors[l])

        glevels = Variable(glevels)

        gx = straight_through_estimator(levels[0], self.residual_errors[0])
        for l in range(levels.shape[0] - 1):
            gx += straight_through_estimator(levels[l+1], self.residual_errors[l+1])

        gx = ga * gx

        return gx, glevels


# Wrapper Functions
def deterministic_binarization(a):
    a_b, = DeterministicBinarization().apply((a, ))
    return a_b


def straight_through_estimator(a, z):
    ste, = StraightThroughEstimator().apply((a, z))
    return ste


def stochastic_binarization(a):
    a_b, = StochasticBinarization().apply((a,))
    return a_b


def residual_binarization(a, levels):
    a_b, = ResidualBinarization().apply((a, levels))
    return a_b