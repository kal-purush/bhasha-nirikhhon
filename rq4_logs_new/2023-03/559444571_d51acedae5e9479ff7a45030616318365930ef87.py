import numpy as np
import open3d as o3d 
import os 
import struct 


path_idx_intensity = "/media/parvez/C00A-4B52/20230313_155258_RecFile_1@20230313_155258219520/hdl_64e_s2_convert_to_XYZ_1_calib_intensities/RecFile_1_20230313_155258@20230313_155258219520_hdl_64e_s2_convert_to_XYZ_1_calib_intensities.idx" 

path_bin_intensity = "/media/parvez/C00A-4B52/20230313_155258_RecFile_1@20230313_155258219520/hdl_64e_s2_convert_to_XYZ_1_calib_intensities/RecFile_1_20230313_155258@20230313_155258219520_hdl_64e_s2_convert_to_XYZ_1_calib_intensities.bin" 



N = 100
intensity = []

with open(path_idx_intensity, "rb") as f:
   f.seek(N*8, 0)
   start_pos = struct.unpack('<Q', f.read(8))[0]
   f.seek((N+1) * 8, 0)
   end_pos = struct.unpack('<Q', f.read(8))[0]
   size = int((end_pos - start_pos) / 4)       
   
with open(path_bin_intensity, "rb") as fb :
   fb.seek(start_pos, 0)
   
   for k in range(size):
      intensity.append(struct.unpack("i", fb.read(4))[0])
   

path_idx_xyz = "/media/parvez/C00A-4B52/20230313_155258_RecFile_1@20230313_155258219520/hdl_64e_s2_convert_to_XYZ_1_XYZ_points/RecFile_1_20230313_155258@20230313_155258219520_hdl_64e_s2_convert_to_XYZ_1_XYZ_points.idx"

path_bin_xyz = "/media/parvez/C00A-4B52/20230313_155258_RecFile_1@20230313_155258219520/hdl_64e_s2_convert_to_XYZ_1_XYZ_points/RecFile_1_20230313_155258@20230313_155258219520_hdl_64e_s2_convert_to_XYZ_1_XYZ_points.bin"


points = []

with open(path_idx_xyz, "rb") as f :
   f.seek(N * 8, 0)
   start_pos = struct.unpack('<Q', f.read(8))[0]
   f.seek((N+1) * 8, 0)
   end_pos = struct.unpack('<Q', f.read(8))[0] 
   size = int((end_pos - start_pos) / 8 )
   
with open(path_bin_xyz, "rb") as fb:
   fb.seek(start_pos, 0)
   
   for k in range(size):
      points.append(struct.unpack("<d", fb.read(8))[0])

num_points = int(len(points)/3)      
points = np.array(points).reshape((num_points, 3))

print(len(intensity))
print(num_points)


intensity = np.array(intensity).reshape((num_points, 1))
print(intensity.shape)
print(points.shape)

print(intensity)

pcd = o3d.t.geometry.PointCloud()

pcd.point["positions"] = o3d.core.Tensor(points)
pcd.point["intensities"] = o3d.core.Tensor(intensity)

o3d.t.io.write_point_cloud(os.path.join("/home/parvez","pointcloud_new.pcd"), pcd)

pcd = o3d.geometry.PointCloud()
pcd.points = o3d.utility.Vector3dVector(points)

#print(pcd)
#print(np.asarray(pcd.points))
o3d.visualization.draw_geometries([pcd])
   

   
   
   
   
   
   
   
 
   