from Qubit import Qubit


# Create a simple Qubit
q1 = Qubit(1, 0)

# Print the Qubit
print("Print example:\nq1: " + str(q1) + "\n") 

# Copy your Qubit
q2 = q1
print("Copy example:\nq2: " + str(q2) + "\n")

# Use pauliX matrix
q3 = q1.pauliX()
print("PauliX example:\nq3: " + str(q3) + "\n") 

# Use pauliY matrix
q4 = q1.pauliY()
print("PauliY example:\nq4: " + str(q4) + "\n")

# Use pauliZ matrix
q5 = q1.pauliZ()
print("PauliZ example:\nq5: " + str(q5) + "\n")

# Use hadamard exmaples
q6 = q1.hadamard()
print("Hadamard example:\nq6: " + str(q6) + "\n")


