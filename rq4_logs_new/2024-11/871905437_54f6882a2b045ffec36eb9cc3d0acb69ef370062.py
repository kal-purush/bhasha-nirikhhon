from typing import List

import numpy as np
from matplotlib import pyplot as plt, animation

from ex_8_aco.aco_tsp_solver import Ant


def visualize_ant_path(ant: Ant,
                       cities: list,
                       x_size: float = 1000,
                       y_size: float = 1000,
                       city_seed: int = None):
    fig, ax = plt.subplots(figsize=(12, 8))

    ax.set_xlim(0, x_size)
    ax.set_ylim(0, y_size)

    title = 'Ant Path Visualization\n'
    city_seed_text = str(city_seed) if city_seed is not None else "None"
    title += f'City Seed: {city_seed_text}'
    ax.set_title(title)

    for i, city in enumerate(cities):
        ax.scatter(city.x, city.y, c='black', s=100, zorder=3)
        ax.annotate(f'{i}', (city.x + 1, city.y + 1), zorder=3)

    path_coords = np.array([(cities[city_idx].x, cities[city_idx].y)
                            for city_idx in ant.visited_cities])

    ax.plot(path_coords[:, 0], path_coords[:, 1],
            color='#4ECDC4',
            alpha=1.0,
            linewidth=2,
            zorder=2,
            label='Ant Path')

    ax.legend(loc='upper right')

    ax.text(0.02, 0.98, f'Distance: {ant.distance:.2f}',
            transform=ax.transAxes,
            verticalalignment='top')

    plt.savefig(f'Outputs/ant_path-seed{city_seed}.png')
    plt.close()


def visualize_ant_evolution(ants: List[Ant],
                            cities: list,
                            x_size: float = 1000,
                            y_size: float = 1000,
                            interval: int = 1000,
                            final_frame_duration: int = 3000,
                            city_seed: int = None):
    fig, ax = plt.subplots(figsize=(12, 8))

    colors = ['#4ECDC4', '#FF6B6B']
    alphas = [1.0, 0.3]

    significant_ants = []
    prev_distance = float('inf')

    for idx, ant in enumerate(ants):
        current_distance = ant.distance
        if current_distance < prev_distance:
            significant_ants.append((idx, ant))
            prev_distance = current_distance

    def init():
        ax.clear()
        ax.set_xlim(0, x_size)
        ax.set_ylim(0, y_size)
        return []

    def animate(frame):
        ax.clear()

        ax.set_xlim(0, x_size)
        ax.set_ylim(0, y_size)

        total_normal_frames = len(significant_ants) * 2

        if frame >= total_normal_frames:
            actual_ant_index = len(significant_ants) - 1
            is_comparison_frame = False
        else:
            is_comparison_frame = frame % 2 == 0
            actual_ant_index = frame // 2

        current_iteration, current_ant = significant_ants[actual_ant_index]

        title = f'Iteration {current_iteration + 1} of {len(ants)}\n'
        city_seed_text = str(city_seed) if city_seed is not None else "None"
        title += f'City Seed: {city_seed_text}'
        ax.set_title(title)

        for i, city in enumerate(cities):
            ax.scatter(city.x, city.y, c='black', s=100, zorder=3)
            ax.annotate(f'{i}', (city.x + 1, city.y + 1), zorder=3)

        if is_comparison_frame and actual_ant_index > 0:
            for hist_idx in range(2):
                if actual_ant_index - hist_idx >= 0:
                    iteration_idx, ant = significant_ants[actual_ant_index - hist_idx]
                    path_coords = np.array([(cities[city_idx].x, cities[city_idx].y)
                                            for city_idx in ant.visited_cities])

                    ax.plot(path_coords[:, 0], path_coords[:, 1],
                            color=colors[hist_idx],
                            alpha=alphas[hist_idx],
                            linewidth=2,
                            zorder=2 - hist_idx,
                            label=f'Iteration {iteration_idx + 1}')
        else:
            iteration_idx, ant = significant_ants[actual_ant_index]
            path_coords = np.array([(cities[city_idx].x, cities[city_idx].y)
                                    for city_idx in ant.visited_cities])

            ax.plot(path_coords[:, 0], path_coords[:, 1],
                    color=colors[0],
                    alpha=alphas[0],
                    linewidth=2,
                    zorder=2,
                    label=f'Iteration {iteration_idx + 1}')

        ax.legend(loc='upper right')

        current_distance = current_ant.distance
        ax.text(0.02, 0.98, f'Distance: {current_distance:.2f}',
                transform=ax.transAxes,
                verticalalignment='top')

        return []

    total_normal_frames = len(significant_ants) * 2
    extra_frames = final_frame_duration // interval
    total_frames = total_normal_frames + extra_frames

    anim = animation.FuncAnimation(fig,
                                   animate,
                                   init_func=init,
                                   frames=total_frames,
                                   interval=interval,
                                   blit=True)

    anim.save(f'Outputs/ant_evolution-seed{city_seed}.gif', writer='pillow')
    plt.close()
    return anim