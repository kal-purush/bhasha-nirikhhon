from dolfin import *
import numpy as np

def line_integral(u, A, B, n): 
    '''Integrate u over segment [A, B] partitioned into n elements'''
    assert u.value_rank() == 0
    assert len(A) == len(B) > 1 and np.linalg.norm(A-B) > 0
    assert n > 0

    # Mesh line for integration
    mesh_points = [A + t*(B-A) for t in np.linspace(0, 1, n+1)]
    tdim, gdim = 1, len(A)
    mesh = Mesh()
    editor = MeshEditor()
    editor.open(mesh, 'interval', tdim, gdim) # 'triangle'?
    editor.init_vertices(n+1)
    editor.init_cells(n)

    for vi, v in enumerate(mesh_points): editor.add_vertex(vi, v)

    for ci in range(n): editor.add_cell(ci, np.array([ci, ci+1], dtype='uintp'))

    editor.close()

    # Setup funcion space
    elm = u.function_space().ufl_element()
    family = elm.family()
    degree = elm.degree()
    V = FunctionSpace(mesh, family, degree)
    v = interpolate(u, V)

    return assemble(v*dx)


mesh = UnitSquareMesh(4,4)

Vh = FunctionSpace(mesh, 'CG', 1)
f = Expression('x[0]+x[1]', domain = mesh, degree=1)

vh = interpolate(f,Vh)


A = np.array([0, 0])
B = np.array([0, 1])

ans = line_integral(vh, A, B, n=100)
ans0 = 0.5
print('Error', abs(ans-ans0))