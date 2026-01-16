import numpy as np
from utils.variables import scaling, px, centre, X_mm, Y_mm, hemisphere1_cuboid_size, hemisphere1_cuboid_height, hemisphere1_dome_radius
from utils.plot import plot_surface

def construct_hemisphere1(scaling,px,centre,hemisphere_cuboid_size,hemisphere_cuboid_height,dome_radius):     
    #Empty heightmap
    heightmap = np.zeros((px,px))

    #Loop over each pixel to generate the height
    for i in range(px):
        #Get radial distance from centre
        for j in range(px):
            dx = i-centre
            dy = j-centre
            distance_from_centre = np.sqrt(dx**2+dy**2)
            #Inside dome radius, find height using eqn of hemisphere
            if distance_from_centre < dome_radius:
                heightmap[i, j] =  hemisphere_cuboid_height + np.sqrt(12.5**2 - (distance_from_centre/scaling)**2)
            elif distance_from_centre == dome_radius:
                heightmap[i,j] = hemisphere_cuboid_height
            #On the cuboid square face set height =10mm
            elif abs(dx) <= hemisphere_cuboid_size/2 and abs(dy) <= hemisphere_cuboid_size/2:
                heightmap[i,j] = hemisphere_cuboid_height

    heightmap = heightmap/np.max(heightmap)
    #heightmap = np.expand_dims(heightmap, axis=0)  #gtb heightmap becomes (1, 128, 128)
    return heightmap

if __name__ == "__main__":
    heightmap = construct_hemisphere1(scaling,px,centre,hemisphere1_cuboid_size, hemisphere1_cuboid_height, hemisphere1_dome_radius)
    plot_surface(X_mm, Y_mm, heightmap, "3D Surface of Hemisphere", zlim=1)