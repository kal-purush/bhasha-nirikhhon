import networkx as nx
import state as st
import matplotlib.pyplot as plt
import random as rd


def get_state_node(graph, state):
    # If the state is already in the graph, return it
    for node in graph:
        if node.compare(state):
            return node
    # If the state was not found, add it and return it
    graph.add_node(state)
    return state


state_counter = 1
# Create the initial state and add to the graph
current_state = st.State([0, 1, 0, 0, 0, 0], state_counter)

# Pending states are created when there are multiple resolutions
pending_states = []

graph = nx.DiGraph()
graph.add_node(current_state)

passes = 0

while True:
    passes += 1

    if current_state.has_influence_conflicts():
        state1, state2 = current_state.get_conflicting_states()
        next_state = state1.next_state()
        pending_states.append((current_state, state2))
    else:
        next_state = current_state.next_state()
    
    next_state.resolve_dependencies()
    graph.add_edge(get_state_node(graph, current_state), get_state_node(graph, next_state))

    if not next_state.has_interstate_transition() and len(pending_states) > 0:
        current_state, pend_state = pending_states.pop()
        next_state = pend_state.next_state()
        next_state.resolve_dependencies()
        graph.add_edge(get_state_node(graph, current_state), get_state_node(graph, next_state))
        
    if rd.random() > 0.5:
        new_state = st.State(next_state.to_list())
        new_state.flip_inflow_der()
        graph.add_edge(get_state_node(graph, next_state), get_state_node(graph, new_state))
        current_state = new_state
        continue

    current_state = next_state

    if passes >= 50:
        break

print(len(graph))
for node in graph:
    print(node.to_list())
nx.draw(graph)
plt.show()