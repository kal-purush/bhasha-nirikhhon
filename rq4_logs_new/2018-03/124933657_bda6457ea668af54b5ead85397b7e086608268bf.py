import numpy
from Shadow import ShadowTools as ST
from silx.gui.plot.StackView import StackViewMainWindow
from oasys.widgets import gui as oasysgui

def plot_data1D(dataX, dataY, title="", xtitle="", ytitle=""):

    plot_canvas = oasysgui.plotWindow()
    plot_canvas.addCurve(dataX, dataY)

    plot_canvas.resetZoom()
    plot_canvas.setXAxisAutoScale(True)
    plot_canvas.setYAxisAutoScale(True)
    plot_canvas.setGraphGrid(False)

    plot_canvas.setXAxisLogarithmic(False)
    plot_canvas.setYAxisLogarithmic(False)
    plot_canvas.setGraphXLabel(xtitle)
    plot_canvas.setGraphYLabel(ytitle)
    plot_canvas.setGraphTitle(title)

    plot_canvas.show()

def plot_data3D(data3D, dataScan, dataX, dataY, title="", xtitle="", ytitle=""):

    xmin = numpy.min(dataX)
    xmax = numpy.max(dataX)
    ymin = numpy.min(dataY)
    ymax = numpy.max(dataY)

    stepX = dataX[1]-dataX[0]
    stepY = dataY[1]-dataY[0]
    if len(dataScan) > 1: stepScan = dataScan[1]-dataScan[0]
    else: stepScan = 1.0

    if stepScan == 0.0: stepScan = 1.0
    if stepX == 0.0: stepX = 1.0
    if stepY == 0.0: stepY = 1.0

    dim0_calib = (dataScan[0], stepScan)
    dim1_calib = (ymin, stepY)
    dim2_calib = (xmin, stepX)

    data_to_plot = numpy.swapaxes(data3D,1,2)

    colormap = {"name":"temperature", "normalization":"linear", "autoscale":True, "vmin":0, "vmax":0, "colors":256}

    plot_canvas = StackViewMainWindow()

    plot_canvas.setGraphTitle(title)
    plot_canvas.setLabels(["Scanned Variable", ytitle, xtitle])
    plot_canvas.setColormap(colormap=colormap)

    plot_canvas.setStack(numpy.array(data_to_plot), calibrations=[dim0_calib, dim1_calib, dim2_calib] )
        
    plot_canvas.show()


shadow_beam = in_object_1

# TO ACCESS THE OPTICAL ELEMENT THAT GENERATE THE BEAM AT THIS IMAGE PLANE
# YOU NEED TO SURF THE HISTORY OF THE BEAM
vls_oe_number = shadow_beam._oe_number
vls_history_element = shadow_beam.getOEHistory(shadow_beam._oe_number)

# SHADOWOUI OBJECTS: WRAPPERS OF THE NATIVE SHADOW3 OBJECTS 
# MAKE A DUPLICATE TO BE SURE TO NOT AFFECT THE REST OF SHADOWOUI SIMULATION!
shadowOui_input_beam = vls_history_element._input_beam.duplicate(history=False)
shadowOui_VLS_before_tracing = vls_history_element._shadow_oe_start.duplicate()
#shadowOui_VLS_after_tracing = vls_history_element._shadow_oe_end.duplicate()

#ticket2D = shadowOui_input_beam._beam.histo2(col_h=1, col_v=3, nbins=100, ref=23)

# VARIABLE TO SCAN IS (JUST AN EXAMPLE) COEFFIECIENT C3 TO OPTIMIZE GRATING

C0 = shadow3_VLS.RULING
C1 = shadow3_VLS.RUL_A1
C2 = shadow3_VLS.RUL_A2
C3 = shadow3_VLS.RUL_A3

print("RULING DENSITY", C0, C1, C2, C3)

# 10 VALUES
c3_values = -5 + numpy.arange(0, 41)*(10/20)

# plotting range
x_min = -0.15
x_max = 0.15
z_min = -0.002
z_max = 0.002 
nbins=101

stack_result = numpy.zeros((len(c3_values), nbins, nbins))
fwhms =  numpy.zeros(len(c3_values))
best_focus_positions = numpy.zeros(len(c3_values))

for index in range(0, len(c3_values)):
    
    # NATIVE SHADOW3 OBJECTS 
    shadow3_beam = shadowOui_input_beam.duplicate()._beam # KEEP THE ORIGINAL UNTOUCHED
    shadow3_VLS = shadowOui_VLS_before_tracing.duplicate()._oe # KEEP THE ORIGINAL UNTOUCHED

    shadow3_VLS.RUL_A3= c3_values[index]
        
    shadow3_beam.traceOE(shadow3_VLS, vls_oe_number) # 
      
    # 2D DISTRIBUTION X,Z TO OBTAIN THE FOCAL SPOT DIMENSION 
    ticket2D = shadow3_beam.histo2(col_h=1, col_v=3, nbins=nbins, ref=23, xrange=[x_min, x_max], yrange=[z_min, z_max])
    
    # FOCUS POSITION
    ticketFocus = ST.focnew(shadow3_beam, mode=0, center=[0.0, 0.0])    
    
    best_focus_positions[index] = ticketFocus['z_waist']
    
    histogram = ticket2D["histogram"]
    fwhms[index] = ticket2D["fwhm_v"]*1e4
        
    for ix in range(nbins):
        for iz in range(nbins):
             stack_result[index, ix, iz] = histogram[ix, iz]


plot_data3D(stack_result, c3_values, numpy.linspace(x_min, x_max, nbins), numpy.linspace(z_min, z_max, nbins), "SPOT SIZE AT IMAGE PLANE", "X [cm]", "Z [cm]")
plot_data1D(c3_values, fwhms, "SPOT VERTICAL SIZE", "C3", "Z FWHM")
plot_data1D(c3_values, best_focus_positions, "VERTICAL FOCUS RELATIVE POSITION", "C3", "Distance")


