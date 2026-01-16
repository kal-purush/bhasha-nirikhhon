#!/usr/bin/env python3
# pylint: disable=C0111


def all_steps(state):
    result = list()
    for action in range(len(state[0])):
        if state[0][action] != 0:
            next_state = jump(state, action, 1) or jump(state, action, 2)
            if next_state:
                result = result + all_steps(next_state)
            else:
                result = result + [state]
    return result


def jump(state, action, distance):
    destination = distance * state[0][action] + action
    if destination < 0 or destination >= len(state[0]):
        return None
    if state[0][destination] != 0:
        return None
    result = list(state[0])
    result[destination] = result[action]
    result[action] = 0
    return result, [action] + state[1]


print("\n".join(["%s : %s" % (x[0], [i for i in reversed(x[1])])
                 for x in all_steps([[1, 1, 1, 0, -1, -1, -1], []])
                 if x[0] == [-1, -1, -1, 0, 1, 1, 1]]))