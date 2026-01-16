import copy

transition_matrix = [
    [0.7, 0.2, 0.05, 0.05],
    [0.6, 0.1, 0.1, 0.2],
    [0.8, 0, 0.2, 0],
    [0.8, 0, 0.2, 0]
]

num_of_steps = 10000

current_vector = [0, 0.5, 0, 0.5]

for step in range(num_of_steps):
    prev_vector = copy.deepcopy(current_vector)
    for i in range(len(current_vector)):

        current_vector[i] = sum(
            [
                prev_vector[j] * transition_matrix[j][i]
                for j in range(len(current_vector))
            ]
        )

print(current_vector)
print(sum(current_vector))