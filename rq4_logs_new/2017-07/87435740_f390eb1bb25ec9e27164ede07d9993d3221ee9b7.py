from trajalign.traj import Traj
from trajalign.average import load_directory
from trajalign.average import average_trajectories

trajectory_list = load_directory(
		path = '../../raw_trajectories/bbc1_deletion/110131_Abp1_bbc1del_MD' , 
		pattern = '.data' ,
		comment_char = '%' , 
		dt = 0.1045 , 
		t_unit = 's' , 
		coord_unit = 'pxl' , 
		frames = 0 , 
		coord = ( 1 , 2 ) , 
		f = 3 , 
		protein = 'Abp1-GFP' , 
		date = '31/01/11' , 
		notes = 'Abp1 trajectory in bbc1 deletion strains. The average is used as a reference to align all other average trajectories from bbc1 delta strains')

best_median , worst_median , aligned_trajectories_median =\
		average_trajectories( trajectory_list , max_frame = 500 , 
				output_file = 'abp1_bbc1del' , median = True , fimax = True )
