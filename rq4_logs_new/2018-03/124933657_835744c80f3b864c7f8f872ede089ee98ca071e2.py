import numpy
from numpy.matlib import repmat
from scipy import stats
from scipy.signal import convolve2d
from oasys.widgets import congruence
from silx.gui.plot import Plot2D, ImageView

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

def plot_2D(x, y, data, xlabel="X [m]", ylabel="Y [m]", title=None):
    xmin, xmax = x.min(), x.max()
    ymin, ymax = y.min(), y.max()

    origin = (xmin, ymin)
    scale = (abs((xmax-xmin)/len(x)), abs((ymax-ymin)/len(y)))

    # PyMCA inverts axis!!!! histogram must be calculated reversed
    data_to_plot = []
    for y_index in range(0, len(y)):
        x_values = []
        for x_index in range(0, len(x)):
            x_values.append(data[x_index, y_index])

        x_values.reverse()

        data_to_plot.append(x_values)
    
    plot_canvas = ImageView()
    plot_canvas.setGraphTitle(title)
    plot_canvas.toolBar()
    
    plot_canvas.addImage(numpy.array(data_to_plot),
                         xlabel=xlabel,
                         ylabel=ylabel,
                         legend="data",
                         origin=origin,
                         scale=scale,
                         replace=True)

    
    return plot_canvas

def combine_distributions(distribution_x, distribution_y):

    coord_x = distribution_x[:, 0]
    coord_y = distribution_y[:, 0]
           
    intensity_x = repmat(distribution_x[:, 1], len(coord_y), 1).transpose() 
    intensity_y = repmat(distribution_y[:, 1], len(coord_x), 1)
    
    #convoluted_intensity = convolve2d(intensity_x, intensity_y, boundary='fill', mode='same', fillvalue=0)
    #convoluted_intensity = numpy.sqrt(intensity_x**2 + intensity_y**2)
    convoluted_intensity = numpy.sqrt(intensity_x*intensity_y)
    #convoluted_intensity = 0.5*(intensity_x + intensity_y)

    return coord_x, coord_y, convoluted_intensity


x_positions = extract_distribution_from_file(distribution_file_name="./x_positions.txt")
z_positions = extract_distribution_from_file(distribution_file_name="./z_positions.txt")
x_divergences = extract_distribution_from_file(distribution_file_name="./x_divergences.txt")
z_divergences = extract_distribution_from_file(distribution_file_name="./z_divergences.txt")

x, z, intensity_source_dimension = combine_distributions(x_positions, z_positions)  
x_first, z_first, intensity_angular_distribution = combine_distributions(x_divergences, z_divergences)    

plot_canvas_1 = plot_2D(x, z, intensity_source_dimension, xlabel="X [m]", ylabel="Z [m]", title="X,Z")
plot_canvas_2 = plot_2D(x_first, z_first, intensity_angular_distribution, xlabel="X' [rad]", ylabel="Z' [rad]", title="AlphaX, AlphaZ")
plot_canvas_1.show()
plot_canvas_2.show()

output = []
output.append(x)
output.append(z)
output.append(intensity_source_dimension)
output.append(x_first)
output.append(z_first)
output.append(intensity_angular_distribution)

