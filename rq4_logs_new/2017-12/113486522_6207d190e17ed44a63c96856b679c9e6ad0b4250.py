INITIAL_STATE = "4,10,4,1,8,4,9,14,5,1,14,15,0,15,3,5"


def reallocate_memory(state):
    state = list(map(int, state.split(",")))  # Convert state to list of int

    index = state.index(max(state))  # Memory bank with the most stuff
    items_to_redistribute = state[index]

    state[index] = 0

    while items_to_redistribute > 0:
        index = (index + 1) % len(state)
        state[index] += 1
        items_to_redistribute -= 1

    return ",".join(map(str, state))


observed_states = set()
current_state = INITIAL_STATE

while True:
    current_state = reallocate_memory(current_state)

    former_len = len(observed_states)
    observed_states.add(current_state)

    if former_len == len(observed_states):
        break

print("Number of steps needed : %s" % (len(observed_states) + 1))