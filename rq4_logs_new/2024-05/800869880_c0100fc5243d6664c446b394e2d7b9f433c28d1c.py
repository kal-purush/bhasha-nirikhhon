import gmsh
import numpy as np
from dolfinx.io import gmshio
from mpi4py import MPI


def generate_2D_mesh(lcar=1.0, W=25.0):
    filename = "Ternay_2D"

    gmsh.initialize()
    geom = gmsh.model.geo

    points = [
        geom.addPoint(-6 * W / 25 + 2, 0, 0, lcar),
        geom.addPoint(19 * W / 25 + 2, 0, 0, lcar),
        geom.addPoint(10 * W / 25 + 2, 12, 0, lcar),
        geom.addPoint(4, 40, 0, lcar),
        geom.addPoint(0, 40, 0, lcar),
        geom.addPoint(0, 20, 0, lcar),
    ]
    major_arc = geom.addPoint(58, -14, 0, lcar)
    center = geom.addPoint(58, 40, 0, lcar)
    boundary = [
        geom.addLine(points[0], points[1]),
        geom.addLine(points[1], points[2]),
        geom.addEllipseArc(points[2], center, major_arc, points[3]),
        geom.addLine(points[3], points[4]),
        geom.addLine(points[4], points[5]),
        geom.addLine(points[5], points[0]),
    ]
    loop = geom.addCurveLoop(boundary)
    surf = geom.addPlaneSurface([loop])

    geom.synchronize()

    gmsh.model.addPhysicalGroup(2, [surf], 1)

    gmsh.model.addPhysicalGroup(1, [boundary[0]], 1)
    gmsh.model.addPhysicalGroup(1, boundary[1:3], 2)
    gmsh.model.addPhysicalGroup(1, [boundary[3]], 3)
    gmsh.model.addPhysicalGroup(1, boundary[4:], 4)
    gmsh.option.setNumber("General.Verbosity", 0)

    gmsh.model.mesh.generate(dim=2)
    # gmsh.write(filename + ".msh")
    # gmsh.write(filename + ".geo_unrolled")

    gdim = 2
    gmsh_model_rank = 0
    mesh_comm = MPI.COMM_WORLD
    mesh, cell_markers, facet_markers = gmshio.model_to_mesh(
        gmsh.model, mesh_comm, gmsh_model_rank, gdim=gdim
    )

    gmsh.finalize()

    return mesh, cell_markers, facet_markers


def generate_2D_cracked_mesh(y_crack=32, a_crack=0.5, lcar=3.0, refinement_ratio=0.1):
    lcar_ref = refinement_ratio * lcar
    e_crack = 0.02

    filename = "Ternay_2D"

    gmsh.initialize()
    geom = gmsh.model.geo

    major_arc = geom.addPoint(58, -14, 0, lcar)
    center = geom.addPoint(58, 40, 0, lcar)

    if y_crack < 20:
        x_crack = -4 * (1 - y_crack / 20)
        points = [
            geom.addPoint(-4, 0, 0, lcar),
            geom.addPoint(21, 0, 0, lcar),
            geom.addPoint(12, 12, 0, lcar),
            geom.addPoint(4, 40, 0, lcar),
            geom.addPoint(0, 40, 0, lcar),
            geom.addPoint(0, 20, 0, lcar),
            geom.addPoint(
                -4 * (1 - (y_crack + e_crack / 2) / 20),
                y_crack + e_crack / 2,
                0,
                lcar_ref,
            ),
            geom.addPoint(-4 * (1 - y_crack / 20), y_crack, 0, lcar_ref),
            geom.addPoint(-4 * (1 - y_crack / 20) + a_crack, y_crack, 0, lcar_ref),
            geom.addPoint(
                -4 * (1 - (y_crack - e_crack / 2) / 20),
                y_crack - e_crack / 2,
                0,
                lcar_ref,
            ),
        ]
        ellipse = geom.addEllipseArc(points[6], points[7], points[8], points[8])
        ellipse2 = geom.addEllipseArc(points[8], points[7], points[8], points[9])
        boundary = [
            geom.addLine(points[0], points[1]),
            geom.addLine(points[1], points[2]),
            geom.addEllipseArc(points[2], center, major_arc, points[3]),
            geom.addLine(points[3], points[4]),
            geom.addLine(points[4], points[5]),
            geom.addLine(points[5], points[6]),
            ellipse,
            ellipse2,
            geom.addLine(points[9], points[0]),
        ]
        crack_faces = [ellipse, ellipse2]
        back_faces = [boundary[4], boundary[5], boundary[8]]
    elif y_crack > 20:
        x_crack = 0
        points = [
            geom.addPoint(-4, 0, 0, lcar),
            geom.addPoint(21, 0, 0, lcar),
            geom.addPoint(12, 12, 0, lcar),
            geom.addPoint(4, 40, 0, lcar),
            geom.addPoint(0, 40, 0, lcar),
            geom.addPoint(0, y_crack + e_crack / 2, 0, lcar_ref),
            geom.addPoint(0, y_crack, 0, lcar_ref),
            geom.addPoint(a_crack, y_crack, 0, lcar_ref),
            geom.addPoint(0, y_crack - e_crack / 2, 0, lcar_ref),
            geom.addPoint(0, 20, 0, lcar),
        ]

        ellipse = geom.addEllipseArc(points[5], points[6], points[7], points[7])
        ellipse2 = geom.addEllipseArc(points[7], points[6], points[7], points[8])
        boundary = [
            geom.addLine(points[0], points[1]),
            geom.addLine(points[1], points[2]),
            geom.addEllipseArc(points[2], center, major_arc, points[3]),
            geom.addLine(points[3], points[4]),
            geom.addLine(points[4], points[5]),
            ellipse,
            ellipse2,
            geom.addLine(points[8], points[9]),
            geom.addLine(points[9], points[0]),
        ]
        crack_faces = [ellipse, ellipse2]
        back_faces = [boundary[4], boundary[7], boundary[8]]
    else:
        raise ValueError("Crack height should be striclty smaller or larger than 20.")

    loop = geom.addCurveLoop(boundary)
    surf = geom.addPlaneSurface([loop])

    geom.synchronize()

    gmsh.model.addPhysicalGroup(2, [surf], 1)

    gmsh.model.addPhysicalGroup(1, [boundary[0]], 1)
    gmsh.model.addPhysicalGroup(1, boundary[1:3], 2)
    gmsh.model.addPhysicalGroup(1, [boundary[3]], 3)
    gmsh.model.addPhysicalGroup(1, back_faces, 4)
    gmsh.model.addPhysicalGroup(1, crack_faces, 5)

    # Create a new scalar field
    field_tag = gmsh.model.mesh.field.add("Box")
    gmsh.model.mesh.field.setNumber(field_tag, "VIn", lcar_ref)
    gmsh.model.mesh.field.setNumber(field_tag, "VOut", lcar)
    gmsh.model.mesh.field.setNumber(field_tag, "XMin", x_crack)
    gmsh.model.mesh.field.setNumber(field_tag, "XMax", x_crack + a_crack + 4)
    gmsh.model.mesh.field.setNumber(field_tag, "YMin", y_crack - 4 * e_crack)
    gmsh.model.mesh.field.setNumber(field_tag, "YMax", y_crack + 4 * e_crack)

    gmsh.model.mesh.field.setAsBackgroundMesh(field_tag)

    gmsh.option.setNumber("General.Verbosity", 0)

    gmsh.model.mesh.generate(dim=2)
    # gmsh.write(filename + ".msh")
    # gmsh.write(filename + ".geo_unrolled")

    gdim = 2
    gmsh_model_rank = 0
    mesh_comm = MPI.COMM_WORLD
    mesh, cell_markers, facet_markers = gmshio.model_to_mesh(
        gmsh.model, mesh_comm, gmsh_model_rank, gdim=gdim
    )

    gmsh.finalize()
    return mesh, cell_markers, facet_markers


def generate_3D_mesh(lcar=1.0, fix_extremities=True):
    filename = "Ternay_3D"

    gmsh.initialize()
    geom = gmsh.model.geo

    points = [
        geom.addPoint(-4, 0, 0, lcar),
        geom.addPoint(21, 0, 0, lcar),
        geom.addPoint(12, 12, 0, lcar),
        geom.addPoint(4, 40, 0, lcar),
        geom.addPoint(0, 40, 0, lcar),
        geom.addPoint(0, 20, 0, lcar),
    ]
    major_arc = geom.addPoint(58, -14, 0, lcar)
    center = geom.addPoint(58.0, 40, 0, lcar)
    boundary = [
        geom.addLine(points[0], points[1]),
        geom.addLine(points[1], points[2]),
        geom.addEllipseArc(points[2], center, major_arc, points[3]),
        geom.addLine(points[3], points[4]),
        geom.addLine(points[4], points[5]),
        geom.addLine(points[5], points[0]),
    ]
    loop = geom.addCurveLoop(boundary)
    surf = geom.addPlaneSurface([loop])

    gmsh.option.setNumber("General.Verbosity", 0)

    N = 20
    theta = np.arctan(16 / 75)
    R = 150 / 2 / np.sin(theta)
    out_dim_tags = geom.revolve([(2, surf)], R, 0, 0, 0, 1, 0, theta, numElements=[N])

    vol = out_dim_tags[1][1]
    bottom = [out_dim_tags[2]]
    front_face = out_dim_tags[3:5]
    top = [out_dim_tags[5]]
    back_face = out_dim_tags[-2:]
    ends = [out_dim_tags[0]]

    out_dim_tags = geom.revolve([(2, surf)], R, 0, 0, 0, 1, 0, -theta, numElements=[N])
    vol2 = out_dim_tags[1][1]
    bottom += [out_dim_tags[2]]
    front_face += out_dim_tags[3:5]
    top += [out_dim_tags[5]]
    back_face += out_dim_tags[-2:]
    ends += [out_dim_tags[0]]

    geom.synchronize()

    gmsh.model.addPhysicalGroup(3, [vol, vol2], 1)
    gmsh.model.addPhysicalGroup(2, [b[1] for b in bottom], 1)
    gmsh.model.addPhysicalGroup(2, [b[1] for b in front_face], 2)
    gmsh.model.addPhysicalGroup(2, [b[1] for b in top], 3)
    gmsh.model.addPhysicalGroup(2, [b[1] for b in back_face], 4)
    if fix_extremities:
        gmsh.model.addPhysicalGroup(2, [b[1] for b in ends], 6)

    gmsh.option.setNumber("General.Verbosity", 0)

    gmsh.model.mesh.generate(dim=3)
    # gmsh.write(filename + ".msh")
    # gmsh.write(filename + ".geo_unrolled")

    gdim = 3
    gmsh_model_rank = 0
    mesh_comm = MPI.COMM_WORLD
    mesh, cell_markers, facet_markers = gmshio.model_to_mesh(
        gmsh.model, mesh_comm, gmsh_model_rank, gdim=gdim
    )

    gmsh.finalize()
    return mesh, cell_markers, facet_markers