# -*- coding: utf-8 -*-
"""
Created on Tue Nov  8 14:34:55 2022

Goal:
    Make changes of coordinate ferom GPS coordinate to whatever is cool
@author: mcaoue2
"""

import numpy as np

def rotate_pts(pts, n_vec, angle):
    """
    
    Apply a rotation from an axis and a angle. 
    Thanks to wikipedia:
    https://en.wikipedia.org/wiki/Rotation_matrix#Rotation_matrix_from_axis_and_angle
    Parameters
    ----------
    pts : list (x,y,z)
        3D point to rotate.
    n_vec : list(nx, ny, nz)
        Vectore pointing in the direction of the axis to rotate about.
    angle : float
        Angle to rotate. In radian
    Returns
    -------
    list (x', y', z')
        The new points.
    """
    # From
    # https://en.wikipedia.org/wiki/Rotation_matrix#Rotation_matrix_from_axis_and_angle
    # But note that we could also use the Rodriguess rotation formalism, which
    # is intuitive (and equivalent I suppose)
    # https://en.wikipedia.org/wiki/Rodrigues%27_rotation_formula   
    # Get the normalized vector
    ux, uy, uz = np.array(n_vec)/np.linalg.norm(n_vec)
    
    cosA = np.cos(angle)
    sinA = np.sin(angle)
    
    # Build the rotation matrix
    R11 = cosA + ux**2*(1-cosA)
    R12 = ux*uy*(1-cosA) - uz*sinA
    R13 = ux*uz*(1-cosA) + uy*sinA
    
    R21 = uy*ux*(1-cosA) + uz*sinA
    R22 = cosA + uy**2*(1-cosA)
    R23 = uy*uz*(1-cosA) - ux*sinA
    
    R31 = uz*ux*(1-cosA) - uy*sinA
    R32 = uz*uy*(1-cosA) + ux*sinA
    R33 = cosA + uz**2*(1-cosA)
    
    rot_matrix = np.matrix([[R11, R12, R13],
                            [R21, R22, R23],
                            [R31, R32, R33]])
    
    # Multiply the point by the rotation matrix
    xn = np.dot(rot_matrix[0], pts)
    yn = np.dot(rot_matrix[1], pts)
    zn = np.dot(rot_matrix[2], pts)
    return  (np.array(xn)[0], 
             np.array(yn)[0], 
             np.array(zn)[0])

class Coordinate():
    """
    Goal:
    Make changes of coordinate from GPS coordinate to whatever is cool
    """
    def __init__(self):
        """
        Nothing to initiate for this version
        """
    def GPS_2_cartesian(self, 
                      longitude, latitude, elevation,
                      R_planet=6371*1e3 ):
        """
        Convert the input GPS coordinate into a cartesian coordinate. 
        The new coordinate, (x, y, z) have z=0 x-y plane to be the plane 
        tangent to the earth surface at the initial point. And the z is the 
        elevation relative to this plane. 
        THere is no streching of the coordinate. Such that it should work on 
        the North pole and at the equator very well.         

        Note: "longitude, latitude, elevation" must have the same lenght if 
            they are arrays !
        
        Parameters
        ----------
        longitude : Float or numpy array of float. 
            Longitude coordinate. In degree.
        latitude :  Float or numpy array of float. 
            Latitude coordinate. In degree.
        elevation : Float or numpy array of float. 
            Elevation above the surface. In meter.
        R_planet: float, optional
            Radius of the planet, assuming a sphere. The default is 6371*1e3.

        Returns
        -------
        x : Float or numpy array of float. 
            X coordinate
        y : Float or numpy array of float. 
            Y coordinate
        z : Float or numpy array of float. 
            Z coordinate

        """
        
        self.lon, self.lat, self.ele = longitude, latitude, elevation
        self.R_planet = R_planet
        
        # =============================================================================
        # Step 1: Converte the spherical to the cartesian        
        # =============================================================================
        deg2rad = np.pi/180
        # Sphereical angle, in radian
        phi   = self.lon*deg2rad
        theta = (90-self.lat)*deg2rad # The original GPS is with respect to the equator
        r = self.R_planet + self.ele 
        self.x1 = r*np.sin(theta)*np.cos(phi)
        self.y1 = r*np.sin(theta)*np.sin(phi)
        self.z1 = r*np.cos(theta)
        
        # =============================================================================
        # Step 2: Rotate the coordinate to make the x-y plane tangent to the surface   
        # =============================================================================
        
        # We are bring the point to the North pole (ie z axis)
        # Reference point to bring at the North Pole
        n_hat = [self.x1[0], self.y1[0], self.z1[0]] 
        # NORMALIZE 
        n_hat = np.array(n_hat)/np.linalg.norm(n_hat)
        
        # Rotation axis
        axis_rot = np.cross(n_hat, [0, 0, 1]) # The reference is the z axis
        
        out = rotate_pts([self.x1, self.y1, self.z1], 
                         axis_rot, theta[0])
        self.x2, self.y2, self.z2 = out
        
        # =============================================================================
        # Step 3: Translate the height to make it relative to the ground
        # =============================================================================
        self.x, self.y , self.z = self.x2, self.y2, self.z2-self.R_planet
        
        return self.x, self.y , self.z

if __name__ == '__main__':
    
    deg2rad = np.pi/180
    
    # define some trajectory in spherical coordinate
    s = np.linspace(0, 2, 350)
    R0 = 6371*1e3
    # Test various original angle
    
    theta = (90-45)*deg2rad + 50/R0*np.sin(s*2*2*np.pi)
    phi   = -73*deg2rad + 80/R0*(1+3*s + np.cos(s*2*2*np.pi) )
    r     = R0 + 100-80*(s-np.mean(s))**2 # THis is the heigh that I should get at the end
    x = r*np.sin(theta)*np.cos(phi)
    y = r*np.sin(theta)*np.sin(phi)
    z = r*np.cos(theta)
    
    from trajectory_viewer import TrajectoryViewer
    
    
    trj = TrajectoryViewer(name='On the planet').show()
        
    trj.set_trajectory(s, x, y, z)
    
    self = Coordinate()
    xn, yn, zn = self.GPS_2_cartesian(longitude=phi/deg2rad,  # In deg !
                                      latitude =90-theta/deg2rad, # GPS In deg !
                                      elevation=r-R0,
                                      R_planet=R0)
    trjn = TrajectoryViewer(name='Transformed').show()
    trjn.set_trajectory(s, xn, yn, zn)    
    
    # trj.set_trajectory(s, xn, yn, zn,overwrite=False)
    
    # Test:
    #     - Local trajectory (not much deformation)
    #     - Long trajectory along the globe. 
    #     - Equator
    #     - North-South pole trajectories
        
        
        
        
        
        
        
        
        
        
        
    