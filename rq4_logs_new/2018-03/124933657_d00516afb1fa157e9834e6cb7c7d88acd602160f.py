import numpy
import scipy.stats as stats
from oasys.widgets import congruence

import matplotlib
import matplotlib.pyplot as plt

matplotlib.rcParams['figure.max_open_warning'] = '0'

class Distribution:
    POSITION = 0
    DIVERGENCE = 1
    
class Axis:
    X = 0
    Z = 1

def extract_distribution_from_file(distribution_file_name):
    distribution = []

    try:
        distribution_file = open(distribution_file_name, "r")

        rows = distribution_file.readlines()

        for index in range(0, len(rows)):
            row = rows[index]

            if not row.strip() == "":
                values = row.split()

                if not len(values) == 2: raise Exception("Malformed file, must be: <value> <spaces> <frequency>")
                               
                value = float(values[0].strip())
                frequency = float(values[1].strip())

                distribution.append([value, frequency])

    except Exception as err:
        raise Exception("Problems reading distribution file: {0}".format(err))
    except:
        raise Exception("Unexpected error reading distribution file: ", sys.exc_info()[0])

    return numpy.array(distribution)


def extract_distribution_from_srw(coor, axis, intensity):
    distribution = []

    if axis == Axis.X:
        index_central = int(len(intensity[0, :])/2)
    elif axis == Axis.Z:
        index_central = int(len(intensity[:, 0])/2)

    print("SHAPE", intensity.shape)

    try:
        for index in range(0, len(coor)):                   
            value = coor[index]
            
            if axis == Axis.X:
                index_x = index
                index_y = index_central
            elif axis == Axis.Z:
                index_x = index_central
                index_y = index
                
            frequency = intensity[index_x, index_y]

            distribution.append([value, frequency])

    except Exception as err:
        raise Exception("Problems reading distribution file: {0}".format(err))
    except:
        raise Exception("Unexpected error reading distribution file: ", sys.exc_info()[0])

    return numpy.array(distribution)

def resample_distribution(x_values, y_values, new_dim):
    e_min = x_values[0]
    e_max = x_values[len(x_values)-1]

    new_x_values = e_min + numpy.arange(0, new_dim+1) * (e_max-e_min)/new_dim

    return new_x_values, numpy.interp(new_x_values, x_values, y_values)

def sample_from_distribution(distribution, npoints):
    if distribution[0, 0] == 0:
        y_values = distribution[1:, 1]
        x_values = distribution[1:, 0]
    else:
        y_values = distribution[:, 1]
        x_values = distribution[:, 0]
   
    coefficient = 1.0
    if len(x_values) < 10000:
        coefficient = 10000
        x_values, y_values = resample_distribution(x_values*10000, y_values, 10000)

  
    # normalize distribution function
    y_values /= numpy.max(y_values)
    y_values /= y_values.sum()

    random_generator = stats.rv_discrete(a=numpy.min(x_values), 
                                         b=numpy.max(x_values), 
                                         name='user_defined_distribution', values=(x_values, y_values))
        
    #plt.figure()
    #plt.plot(x_values, y_values, 'r')
    #plt.show()
    
    return random_generator.rvs(size=npoints)/coefficient

def generate_user_defined_distribution(beam_out, 
                                       user_defined_file, 
                                       axis=Axis.X, 
                                       distribution_type=Distribution.POSITION,
                                       minimum_value=-2e-5, 
                                       step=1e-6):
                                       
    distribution = extract_distribution_from_file(congruence.checkFile(user_defined_file))
        
    sampled_distribution = sample_from_distribution(distribution, len(beam_out._beam.rays))
    
    if distribution_type == Distribution.POSITION:
        if axis == Axis.X:
            axis_index = 0 
        elif axis == Axis.Z:
            axis_index = 2 
    elif distribution_type == Distribution.DIVERGENCE:
        if axis == Axis.X:
            axis_index = 4 
        elif axis == Axis.Z:
            axis_index = 6 

    beam_out._beam.rays[:, axis_index] = minimum_value + sampled_distribution*step


def generate_user_defined_distribution_from_srw(beam_out, 
                                                coord,
                                                intensity, 
                                                axis=Axis.X, 
                                                distribution_type=Distribution.POSITION,
                                                minimum_value=-2e-5, 
                                                step=1e-6):
                                       
    distribution = extract_distribution_from_srw(coord, axis, intensity)
        
    sampled_distribution = sample_from_distribution(distribution, len(beam_out._beam.rays))
    
    if distribution_type == Distribution.POSITION:
        if axis == Axis.X:
            axis_index = 0 
        elif axis == Axis.Z:
            axis_index = 2 
    elif distribution_type == Distribution.DIVERGENCE:
        if axis == Axis.X:
            axis_index = 4 
        elif axis == Axis.Z:
            axis_index = 6 

    beam_out._beam.rays[:, axis_index] = minimum_value + sampled_distribution*step


##########################################

shadow_beam = in_object_1

x, z, intensity_source_dimension, x_first, z_first, intensity_angular_distribution = in_object_2

# SWITCH FROM SRW METERS TO SHADOWOUI U.M.
x *= 100
z *= 100

generate_user_defined_distribution_from_srw(beam_out=shadow_beam, 
                                   coord=numpy.arange(0, len(x)),
                                   intensity=intensity_source_dimension, 
                                   axis=Axis.X, 
                                   distribution_type=Distribution.POSITION, 
                                   minimum_value=numpy.min(x), 
                                   step=numpy.abs(x[1]-x[0]))

generate_user_defined_distribution_from_srw(beam_out=shadow_beam, 
                                   coord=numpy.arange(0, len(z)),
                                   intensity=intensity_source_dimension, 
                                   axis=Axis.Z, 
                                   distribution_type=Distribution.POSITION, 
                                   minimum_value=numpy.min(z), 
                                   step=numpy.abs(z[1]-z[0]))

generate_user_defined_distribution_from_srw(beam_out=shadow_beam, 
                                   coord=numpy.arange(0, len(x_first)),
                                   intensity=intensity_angular_distribution, 
                                   axis=Axis.X, 
                                   distribution_type=Distribution.DIVERGENCE, 
                                   minimum_value=numpy.min(x_first), 
                                   step=numpy.abs(x_first[1]-x_first[0]))

generate_user_defined_distribution_from_srw(beam_out=shadow_beam, 
                                   coord=numpy.arange(0, len(z_first)),
                                   intensity=intensity_angular_distribution, 
                                   axis=Axis.Z, 
                                   distribution_type=Distribution.DIVERGENCE, 
                                   minimum_value=numpy.min(z_first), 
                                   step=numpy.abs(z_first[1]-z_first[0]))


'''
x_positions_file = "./x_positions.txt"

generate_user_defined_distribution(beam_out=shadow_beam, 
                                   user_defined_file=x_positions_file, 
                                   axis=Axis.X, 
                                   distribution_type=Distribution.POSITION, 
                                   minimum_value=-2e-5, 
                                   step=1e-6)


z_positions_file = "./z_positions.txt"

generate_user_defined_distribution(beam_out=shadow_beam, 
                                   user_defined_file=z_positions_file, 
                                   axis=Axis.Z, 
                                   distribution_type=Distribution.POSITION, 
                                   minimum_value=-1e-5, 
                                   step=0.5e-6)


x_divergences_file = "./x_divergences.txt"

generate_user_defined_distribution(beam_out=shadow_beam, 
                                   user_defined_file=x_divergences_file, 
                                   axis=Axis.X, 
                                   distribution_type=Distribution.DIVERGENCE, 
                                   minimum_value=-2e-5, 
                                   step=1e-6)


z_divergences_file = "./z_divergences.txt"

generate_user_defined_distribution(beam_out=shadow_beam, 
                                   user_defined_file=z_divergences_file, 
                                   axis=Axis.Z, 
                                   distribution_type=Distribution.DIVERGENCE, 
                                   minimum_value=-1e-5, 
                                   step=0.5e-6)

'''

out_object = shadow_beam