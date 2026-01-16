# TODO: list of function to be implemented and their signatures

from enum import IntEnum
from ..model import Model, Data
from ..kinematics import jacobian, djacobian


class Space(IntEnum):
    MOTOR = 1
    LOAD = 2


def inertia(model: Model, data: Data, space: Space) -> float:
    """Calculate inertia term D(q) for choosen space"""
    m, I = model.mass, model.inertia

    if space == Space.MOTOR:
        J = jacobian(model, data)
        D = m * J**2 + I
    elif space == Space.LOAD:
        J = jacobian(model, data)
        D = I * (J**-2)
    else:
        raise ValueError(f"Invalid space: {space}")

    return D


def coriolis(model: Model, data: Data, space: Space) -> float:
    """Calculate coriolis term C(q, dq) for choosen space"""
    m, I = model.mass, model.inertia
    b_theta, b_x = model.b_th, model.b_x

    J = jacobian(model, data)
    dJdt = djacobian(model, data)
    if space == Space.MOTOR:
        C = m * J * dJdt + J * b_x + b_theta
    elif space == Space.LOAD:
        C = I * (J**-2) * dJdt + b_x + b_theta * (J**-1)
    else:
        raise ValueError(f"Invalid space: {space}")

    return C


def jamming(model: Model, data: Data, force: float) -> float:
    L, r = model.L, model.r
    C_r, C_L = model.Kr, model.Kl

    theta, X = data.theta, data.x

    dS_dtheta = (r / (L - X) ** 2) ** 2 * (
        (2 * L**2 - (r * theta) ** 2) * r * theta**3 * C_r - L**3 * theta * C_L
    )

    return dS_dtheta * (force**2)


def static(model: Model, data: Data, force: float = 0) -> float:
    J = jacobian(model, data)

    return J * force


def nonlinear(model: Model, data: Data, space: Space) -> float:
    C = coriolis(model, data, space)
    G = static(model, data)

    if space == Space.MOTOR:
        return C * data.dtheta + G
    elif space == Space.LOAD:
        return C * data.dx + G
    else:
        raise ValueError(f"Invalid space: {space}")


"""
# TODO: decide on this function, maybe it is already implemented inside
def nonlinear_term_mixed(theta, dtheta, x, dx, load_params, motor_params, string_params):
    L, r = string_params
    b_m, I = motor_params  
    m, b_x = load_params
    J = 1
    h_x = b_m*dtheta - I*(dtheta**2 * r**2 + dx**2)/((L-x)*J) + J*(b_x*dx +m*g)
    return h_x
"""