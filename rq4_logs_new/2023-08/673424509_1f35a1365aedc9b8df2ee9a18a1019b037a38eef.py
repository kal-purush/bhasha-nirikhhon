import laspy
import numpy as np
import matplotlib.pyplot as plt


# Particle class to store information about a point in 3D space
class Particle:
    def __init__(self, position, intensity):
        self.position = np.array(position)
        self.intensity = intensity


# OctreeCell class to represent a cell in the octree
class OctreeCell:
    def __init__(self, center, size):
        self.center = np.array(center)
        self.size = size
        self.sphere_radius = size / 2
        self.particles = []


# Octree class to construct the octree and perform operations
class Octree:
    def __init__(self, center, size, depth):
        self.root = OctreeCell(center, size)
        self.max_depth = depth

    # Method to insert a particle into the octree
    def insert_particle(self, particle, node=None, depth=0):
        if node is None:
            node = self.root  # If node is not provided, start from the root

        if depth == self.max_depth:
            node.particles.append(particle)  # If reached maximum depth, add particle to the current node
            return

        # Check if the particle is inside the sphere of the current node
        if np.linalg.norm(particle.position - node.center) < node.sphere_radius:
            octant = self.get_octant(particle.position, node.center)  # Determine the octant for the particle

            if not hasattr(node, 'children'):
                # If the node doesn't have children, create eight children for octants
                node.children = [None] * 8
                child_size = node.size / 2
                for i in range(8):
                    # Calculate the center for each child octant based on the offset
                    offset = np.array([(i & 1) * child_size, ((i >> 1) & 1) * child_size, ((i >> 2) & 1) * child_size])
                    child_center = node.center - node.size / 4 + offset
                    # Create the child cell with the calculated center and size
                    node.children[i] = OctreeCell(child_center, child_size)

            # Recursively insert the particle into the appropriate child node
            self.insert_particle(particle, node.children[octant], depth + 1)

    # Method to determine the octant in which a point lies, relative to the center of a cell
    def get_octant(self, point, center):
        """
        In three-dimensional space, an octree divides the space into eight octants
        :param point: The 3D coordinates of the point for which we want to find the octant.
        :param center: The 3D coordinates of the center of the cell in which we are interested.
        :return: an integer (0 to 7) representing the octant in which a given point lies,
        relative to the center of a cell in the octree.
        """
        octant = 0
        if point[0] >= center[0]:
            octant |= 1
        if point[1] >= center[1]:
            octant |= 2
        if point[2] >= center[2]:
            octant |= 4
        return octant

    # Retrieving all particles in the octree
    def get_all_particles(self):
        particles = []
        queue = [self.root]  # Start with the root node

        while queue:
            node = queue.pop(0)
            particles.extend(node.particles)  # Add particles in the current node to the list
            if hasattr(node, 'children'):
                queue.extend(node.children)  # Add children nodes to the queue for processing

        return particles

    # Method to visualize the octree and particles within the embedded spheres
    def visualize(self, num_particles_to_visualize=1000):
        """
        :param num_particles_to_visualize: Maximum particles to visualize. Full file of LAS has alot of particles =
        alot of ram needed
        """
        particles = self.get_all_particles()[:num_particles_to_visualize]

        fig = plt.figure(figsize=(8, 8))
        ax = fig.add_subplot(111, projection='3d')

        for particle in particles:
            ax.scatter(particle.position[0], particle.position[1], particle.position[2], c=[particle.intensity],
                       cmap='jet', marker='o', s=50)

        half_size = self.root.size / 2
        ax.set_xlim([self.root.center[0] - half_size, self.root.center[0] + half_size])
        ax.set_ylim([self.root.center[1] - half_size, self.root.center[1] + half_size])
        ax.set_zlim([self.root.center[2] - half_size, self.root.center[2] + half_size])

        ax.set_xlabel('X')
        ax.set_ylabel('Y')
        ax.set_zlabel('Z')
        ax.set_title('Octree with Particles')


if __name__ == "__main__":
    las = laspy.read("FILE LOCATION")  # Read LAS data file
    data = {
        'X': las.x,
        'Y': las.y,
        'Z': las.z,
        'intensity': las.intensity,
    }
    octree = Octree(center=[np.mean(data['X']), np.mean(data['Y']), np.mean(data['Z'])],
                    size=np.max(data['X']) - np.min(data['X']),
                    depth=3)
    particles = [Particle([data['X'][i], data['Y'][i], data['Z'][i]], data['intensity'][i]) for i in
                 range(len(data['X']))]
    for particle in particles:
        octree.insert_particle(particle)
    print(f"Total particles inserted into the octree: {len(octree.get_all_particles())}")
    octree.visualize()  # Visualize the octree and particles, by default it's 1000 cause that requires alot of RAM..
    plt.show()