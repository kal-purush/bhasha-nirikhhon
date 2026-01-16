from utils.map_tools import get_maps, load_panos
import numpy as np

class semantic_map(object):
    def __init__(
        self, scene_name, resolution, 
        map_root="data/maps/gmap_floor1_mpp_0.05_channel_last_with_bounds",
        pano_path=""
    ) -> None:
        self.resolution = resolution
        self.nav_map, self.room_map, self.obj_maps, self.grid_dimensions, bounds \
            = get_maps(scene_name, map_root)
        
        upper_bound, lower_bound = bounds[0], bounds[1]
        self.lower_bound = lower_bound
        self.grid_size = (
            abs(upper_bound[2] - lower_bound[2]) / self.grid_dimensions[0],
            abs(upper_bound[0] - lower_bound[0]) / self.grid_dimensions[1],
        )
        self.panoramas = load_panos(scene_name, pano_path)
    
    def gridmap2worldloc(self, grid_points):
        """
        Args:
            grid_points: 2D points (N,2)
        Return:
            3D points (N,3)
        """
        world_locs_x = self.lower_bound[2] + grid_points[:, 0]  * self.grid_size[0]
        world_locs_y = self.lower_bound[0] + grid_points[:, 1] * self.grid_size[1]
        world_locs_z = np.zeros_like(world_locs_y)
        return np.hstack((world_locs_x, world_locs_y, world_locs_z))
    
    def worldloc2gridmap(self, world_locs):
        """
        Args:
            worldlocloc: 3D points (N,3)
        Return:
            2D points (N,2)
        """
        grid_x = ((world_locs[:, 0] - self.lower_bound[2]) / self.grid_size[0]).astype(int)
        grid_y = ((world_locs[:, 1] - self.lower_bound[0]) / self.grid_size[1]).astype(int)
        return np.hstack((grid_x, grid_y))
    
    def discretized_grid(self):
        """
        Return:
            grid vertex (N,2)
        """
        pass
    

    def discretize_point(self, grid_points):
        """
        Args:
            grid_points (N,2)
        Return:
            discretized points (N,2) merged
        """
        pass
    
    def discretize_path(self):
        """
        input:    [(K,2)] *N
        output:   [(M,2)] *N
        """
        pass


    def shortest_path(self, src_points, target_points):
        """
        input:    src points (N,2)   target points(N,2)
        output:   [(M,2)] *N
        """
        pass
    
    def get_panorama(self, grid_points):
        """
        Args:
            grid_points: (N,2)
        Return:
            [N, K,K,3]
        """
        return self.panoramas[grid_points[:,0], grid_points[:,1]]

    def get_map(self):
        pass

    def get_map_centered(self):
        """
        Return:
            partial global map centered around start point of agent
            class: semantic_map
            require offset added to left-top coner to selected region
        """
        pass

    def get_map_features(self, model):
        pass
    def get_pano_features(self, model):
        pass

    # RL related func
    def check(self, grid_points):
        """ whether point is a navigable point (on nav map or floor)
        Return:
            (N,1) binary
        """
        return self.nav_map[grid_points[:,0], grid_points[:,1]] > 0 |  self.obj_maps[grid_points[:,0], grid_points[:,1], 1] > 0

