import numpy as np
import matplotlib.pyplot as plt
import cmasher as cmr
from tqdm import tqdm

N_POINTS_Y = 15
ASPECT_RATIO = 20  
KINEMATIC_VISCOSITY = 0.001
TIME_STEP_LENGTH = 0.001
N_TIME_STEPS = 10000  
PLOT_EVERY = 100
N_PRESSURE_POISSON_ITERATIONS = 50

def main():
    cell_length = 1.0 / (N_POINTS_Y - 1)
    n_points_x = (N_POINTS_Y - 1) * ASPECT_RATIO + 1

    x_range = np.linspace(0.0, 1.0 * ASPECT_RATIO, n_points_x)
    y_range = np.linspace(0.0, 1.0, N_POINTS_Y)

    coordinates_x, coordinates_y = np.meshgrid(x_range, y_range)

    # Initial condition
    velocity_x_prev = np.ones((N_POINTS_Y + 1, n_points_x))
    velocity_x_prev[0, :] = - velocity_x_prev[1, :]
    velocity_x_prev[-1, :] = - velocity_x_prev[-2, :]

    velocity_y_prev = np.zeros((N_POINTS_Y, n_points_x + 1))

    pressure_prev = np.zeros((N_POINTS_Y + 1, n_points_x + 1))

    # Pre-Allocate some arrays
    velocity_x_tent = np.zeros_like(velocity_x_prev)
    velocity_x_next = np.zeros_like(velocity_x_prev)

    velocity_y_tent = np.zeros_like(velocity_y_prev)
    velocity_y_next = np.zeros_like(velocity_y_prev)

    plt.style.use("dark_background")
    plt.figure(figsize=(1.5 * ASPECT_RATIO, 6))

    
    obstacle_center_x = n_points_x // 4
    obstacle_center_y = N_POINTS_Y // 2
    obstacle_radius = N_POINTS_Y // 6

    for iter in tqdm(range(N_TIME_STEPS)):
        # Update interior of u velocity
        diffusion_x = KINEMATIC_VISCOSITY * (
            (
                +
                velocity_x_prev[1:-1, 2:]
                +
                velocity_x_prev[2:, 1:-1]
                +
                velocity_x_prev[1:-1, :-2]
                +
                velocity_x_prev[:-2, 1:-1]
                - 4 *
                velocity_x_prev[1:-1, 1:-1]
            ) / (
                cell_length ** 2
            )
        )
        convection_x = (
            (
                velocity_x_prev[1:-1, 2:] ** 2
                -
                velocity_x_prev[1:-1, :-2] ** 2
            ) / (
                2 * cell_length
            )
            +
            (
                velocity_y_prev[1:, 1:-2]
                +
                velocity_y_prev[1:, 2:-1]
                +
                velocity_y_prev[:-1, 1:-2]
                +
                velocity_y_prev[:-1, 2:-1]
            ) / 4
            *
            (
                velocity_x_prev[2:, 1:-1]
                -
                velocity_x_prev[:-2, 1:-1]
            ) / (
                2 * cell_length
            )
        )
        pressure_gradient_x = (
            (
                pressure_prev[1:-1, 2:-1]
                -
                pressure_prev[1:-1, 1:-2]
            ) / (
                cell_length
            )
        )

        velocity_x_tent[1:-1, 1:-1] = (
            velocity_x_prev[1:-1, 1:-1]
            +
            TIME_STEP_LENGTH
            *
            (
                -
                pressure_gradient_x
                +
                diffusion_x
                -
                convection_x
            )
        )

        # Apply BC
        velocity_x_tent[1:-1, 0] = 1.0
        velocity_x_tent[1:-1, -1] = velocity_x_tent[1:-1, -2]
        velocity_x_tent[0, :] = - velocity_x_tent[1, :]
        velocity_x_tent[-1, :] = - velocity_x_tent[-2, :]

        # Apply obstacle BC for velocity in x direction
        for i in range(N_POINTS_Y + 1):
            for j in range(n_points_x):
                if np.sqrt((i - obstacle_center_y)**2 + (j - obstacle_center_x)**2) <= obstacle_radius:
                    velocity_x_tent[i, j] = 0.0

        # Update interior of v velocity
        diffusion_y = KINEMATIC_VISCOSITY * (
            (
                +
                velocity_y_prev[1:-1, 2:]
                +
                velocity_y_prev[2:, 1:-1]
                +
                velocity_y_prev[1:-1, :-2]
                +
                velocity_y_prev[:-2, 1:-1]
                -
                4 * velocity_y_prev[1:-1, 1:-1]
            ) / (
                cell_length ** 2
            )
        )
        convection_y = (
            (
                velocity_x_prev[2:-1, 1:]
                +
                velocity_x_prev[2:-1, :-1]
                +
                velocity_x_prev[1:-2, 1:]
                +
                velocity_x_prev[1:-2, :-1]
            ) / 4
            *
            (
                velocity_y_prev[1:-1, 2:]
                -
                velocity_y_prev[1:-1, :-2]
            ) / (
                2 * cell_length
            )
            +
            (
                velocity_y_prev[2:, 1:-1] ** 2
                -
                velocity_y_prev[:-2, 1:-1] ** 2
            ) / (
                2 * cell_length
            )
        )
        pressure_gradient_y = (
            (
                pressure_prev[2:-1, 1:-1]
                -
                pressure_prev[1:-2, 1:-1]
            ) / (
                cell_length
            )
        )

        velocity_y_tent[1:-1, 1:-1] = (
            velocity_y_prev[1:-1, 1:-1]
            +
            TIME_STEP_LENGTH
            *
            (
                -
                pressure_gradient_y
                +
                diffusion_y
                -
                convection_y
            )
        )

        # Apply BC
        velocity_y_tent[1:-1, 0] = - velocity_y_tent[1:-1, 1]
        velocity_y_tent[1:-1, -1] = velocity_y_tent[1:-1, -2]
        velocity_y_tent[0, :] = 0.0
        velocity_y_tent[-1, :] = 0.0

        # Apply obstacle BC for velocity in y direction
        for i in range(N_POINTS_Y):
            for j in range(n_points_x + 1):
                if np.sqrt((i - obstacle_center_y)**2 + (j - obstacle_center_x)**2) <= obstacle_radius:
                    velocity_y_tent[i, j] = 0.0

        # Compute the divergence as it will be the rhs of the pressure poisson problem
        divergence = (
            (
                velocity_x_tent[1:-1, 1:]
                -
                velocity_x_tent[1:-1, :-1]
            ) / (
                cell_length
            )
            +
            (
                velocity_y_tent[1:, 1:-1]
                -
                velocity_y_tent[:-1, 1:-1]
            ) / (
                cell_length
            )
        )
        pressure_poisson_rhs = divergence / TIME_STEP_LENGTH

        # Solve the pressure correction poisson problem
        pressure_correction_prev = np.zeros_like(pressure_prev)
        for _ in range(N_PRESSURE_POISSON_ITERATIONS):
            pressure_correction_next = np.zeros_like(pressure_correction_prev)
            pressure_correction_next[1:-1, 1:-1] = 1 / 4 * (
                +
                pressure_correction_prev[1:-1, 2:]
                +
                pressure_correction_prev[2:, 1:-1]
                +
                pressure_correction_prev[1:-1, :-2]
                +
                pressure_correction_prev[:-2, 1:-1]
                -
                cell_length ** 2
                *
                pressure_poisson_rhs
            )

            # Apply pressure BC: Homogeneous Neumann everywhere except for the right where is a homogeneous Dirichlet
            pressure_correction_next[1:-1, 0] = pressure_correction_next[1:-1, 1]
            pressure_correction_next[1:-1, -1] = - pressure_correction_next[1:-1, -2]
            pressure_correction_next[0, :] = pressure_correction_next[1, :]
            pressure_correction_next[-1, :] = pressure_correction_next[-2, :]

            # Advance in smoothing
            pressure_correction_prev = pressure_correction_next

        # Update the pressure
        pressure_next = pressure_prev + pressure_correction_next

        # Correct the velocities to be incompressible
        pressure_correction_gradient_x = (
            (
                pressure_correction_next[1:-1, 2:-1]
                -
                pressure_correction_next[1:-1, 1:-2]
            ) / (
                cell_length
            )
        )

        velocity_x_next[1:-1, 1:-1] = (
            velocity_x_tent[1:-1, 1:-1]
            -
            TIME_STEP_LENGTH
            *
            pressure_correction_gradient_x
        )

        pressure_correction_gradient_y = (
            (
                pressure_correction_next[2:-1, 1:-1]
                -
                pressure_correction_next[1:-2, 1:-1]
            ) / (
                cell_length
            )
        )

        velocity_y_next[1:-1, 1:-1] = (
            velocity_y_tent[1:-1, 1:-1]
            -
            TIME_STEP_LENGTH
            *
            pressure_correction_gradient_y
        )

        # Again enforce BC
        velocity_x_next[1:-1, 0] = 1.0
        inflow_mass_rate_next = np.sum(velocity_x_next[1:-1, 0])
        outflow_mass_rate_next = np.sum(velocity_x_next[1:-1, -2])
        velocity_x_next[1:-1, -1] = velocity_x_next[1:-1, -2] * inflow_mass_rate_next / outflow_mass_rate_next
        velocity_x_next[0, :] = - velocity_x_next[1, :]
        velocity_x_next[-1, :] = - velocity_x_next[-2, :]

        velocity_y_next[1:-1, 0] = - velocity_y_next[1:-1, 1]
        velocity_y_next[1:-1, -1] = velocity_y_next[1:-1, -2]
        velocity_y_next[0, :] = 0.0
        velocity_y_next[-1, :] = 0.0

        # Apply obstacle BC again for next velocities
        for i in range(N_POINTS_Y + 1):
            for j in range(n_points_x):
                if np.sqrt((i - obstacle_center_y)**2 + (j - obstacle_center_x)**2) <= obstacle_radius:
                    velocity_x_next[i, j] = 0.0
        for i in range(N_POINTS_Y):
            for j in range(n_points_x + 1):
                if np.sqrt((i - obstacle_center_y)**2 + (j - obstacle_center_x)**2) <= obstacle_radius:
                    velocity_y_next[i, j] = 0.0

        # Advance in time
        velocity_x_prev = velocity_x_next
        velocity_y_prev = velocity_y_next
        pressure_prev = pressure_next

        # Visualization
        if iter % PLOT_EVERY == 0:
            velocity_x_vertex_centered = (
                (
                    velocity_x_next[1:, :]
                    +
                    velocity_x_next[:-1, :]
                ) / 2
            )
            velocity_y_vertex_centered = (
                (
                    velocity_y_next[:, 1:]
                    +
                    velocity_y_next[:, :-1]
                ) / 2
            )

            plt.contourf(
                coordinates_x,
                coordinates_y,
                velocity_x_vertex_centered,
                levels=10,
                cmap=cmr.ocean_r,
                vmin=-1.5,
                vmax=1.5,
            )
            plt.colorbar()

            plt.quiver(
                coordinates_x[:, ::6],
                coordinates_y[:, ::6],
                velocity_x_vertex_centered[:, ::6],
                velocity_y_vertex_centered[:, ::6],
                alpha=0.4,
            )

            plt.plot(
                5 * cell_length + velocity_x_vertex_centered[:, 5],
                coordinates_y[:, 5], 
                color="black",
                linewidth=3,
            )
            plt.plot(
                40 * cell_length + velocity_x_vertex_centered[:, 20],
                coordinates_y[:, 40], 
                color="black",
                linewidth=3,
            )
            plt.plot(
                80 * cell_length + velocity_x_vertex_centered[:, 80],
                coordinates_y[:, 80], 
                color="black",
                linewidth=3,
            )
            plt.plot(
                180 * cell_length + velocity_x_vertex_centered[:, 80],
                coordinates_y[:, 180], 
                color="black",
                linewidth=3,
            )

            plt.draw()
            plt.pause(0.15)
            plt.clf()

    plt.figure()
    plt.streamplot(
        coordinates_x,
        coordinates_y,
        velocity_x_vertex_centered,
        velocity_y_vertex_centered,
    )

    plt.show()

if __name__ == "__main__":
    main()