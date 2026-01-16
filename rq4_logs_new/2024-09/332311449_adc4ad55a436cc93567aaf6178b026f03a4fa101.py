from tsa import Model, Data
from tsa.kinematics import contraction, jacobian
import casadi as ca

# Symbolic modeling with CasADi

# Create symbolic model
model = Model()
model.kinematic.length = ca.SX.sym("L")
model.kinematic.radius = ca.SX.sym("r")
print("Symbolic Model:")
print(model)

data = Data()
data.motor.position = ca.SX.sym("theta")
print("Symbolic Data:")
print(data)

# Calculate contraction
x = contraction(model, data)
print("Symbolic Contraction:")
print(x)
print("Contraction equals load position:", x == data.load.position)

# Calculate Jacobian
j = jacobian(model, data)
print("Symbolic Jacobian:")
print(j)
print("Jacobian equals motor jacobian:", j == data.motor.jacobian)

# Create CasADi functions
contraction_func = ca.Function(
    "contraction", [model.kinematic.length, model.kinematic.radius, data.motor.position], [x]
)
jacobian_func = ca.Function("jacobian", [model.kinematic.length, model.kinematic.radius, data.motor.position], [j])

# Calculate derivatives with respect to parameters
dxdL = ca.jacobian(x, model.kinematic.length)
dxdr = ca.jacobian(x, model.kinematic.radius)
print("dx/dL:", dxdL)
print("dx/dr:", dxdr)

# Evaluate functions with numeric values
L = 0.2
r = 1e-3

theta = 100
print("\nNumeric evaluation using CasADi functions:")
print("Contraction:", contraction_func(L, r, theta))
print("Jacobian:", jacobian_func(L, r, theta))