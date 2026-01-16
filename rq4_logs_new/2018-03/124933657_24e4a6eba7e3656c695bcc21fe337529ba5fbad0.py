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
kb_hrm_oe_number = shadow_beam._oe_number
kb_hrm_history_element = shadow_beam.getOEHistory(shadow_beam._oe_number)

# SHADOWOUI OBJECTS: WRAPPERS OF THE NATIVE SHADOW3 OBJECTS 
shadowOui_input_beam = kb_hrm_history_element._input_beam
shadowOui_kb_hrm_before_tracing = kb_hrm_history_element._shadow_oe_start
shadowOui_kb_hrm_after_tracing = kb_hrm_history_element._shadow_oe_end # NEED AFTER! Axis are calculated by shadow after the raytracing!

#ticket2D = shadowOui_input_beam._beam.histo2(col_h=1, col_v=3, nbins=100, ref=23)

# VARIABLE TO SCAN IS (JUST AN EXAMPLE) COEFFIECIENT C3 TO OPTIMIZE GRATING

AXMIN = shadowOui_kb_hrm_after_tracing._oe.AXMIN
AXMAJ = shadowOui_kb_hrm_after_tracing._oe.AXMAJ

SSOUR = shadowOui_kb_hrm_after_tracing._oe.SSOUR
SIMAG = shadowOui_kb_hrm_after_tracing._oe.SIMAG
#### FROM SHADOW3 KERNEL

#! C Computes the mirror center position
#! C
#! C The center is computed on the basis of the object and image positions
#! C

AFOCI 	=  numpy.sqrt(AXMAJ**2-AXMIN**2)
ECCENT 	=  AFOCI/AXMAJ

YCEN  = (SSOUR-SIMAG)*0.5/ECCENT
ZCEN  = -numpy.sqrt(1-YCEN**2/AXMAJ**2)*AXMIN

# ANGLE OF AXMAJ AND POLE (CCW): NOT RETURNED BY SHADOW!
ELL_THE = numpy.degrees(numpy.arctan(numpy.abs(ZCEN/YCEN)))

print("AXMIN, AXMAJ, THE [deg]", AXMIN, AXMAJ, ELL_THE)

# 10 VALUES
axmaj_values = -1 + AXMAJ + numpy.arange(0, 41)*(2/40)

# plotting range
x_min = -0.005
x_max = 0.005
z_min = -0.005
z_max = 0.005 
nbins=501

stack_result = numpy.zeros((len(axmaj_values), nbins, nbins))
fwhms =  numpy.zeros(len(axmaj_values))
best_focus_positions = numpy.zeros(len(axmaj_values))

for index in range(0, len(axmaj_values)):
    
    # NATIVE SHADOW3 OBJECTS 
    # MAKE A DUPLICATE TO BE SURE TO NOT AFFECT THE REST OF SHADOWOUI SIMULATION!
    # Note: TO BEGIN THE SIMULATION THE OE BEFORE THE RAY TRACING IS NEEDED
    shadow3_beam = shadowOui_input_beam.duplicate(copy_rays=True, history=False)._beam # KEEP THE ORIGINAL UNTOUCHED
    shadow3_kb_hrm  = shadowOui_kb_hrm_before_tracing.duplicate()._oe                  # KEEP THE ORIGINAL UNTOUCHED
    
    #switch to external/user defined surface shape parameters
    shadow3_kb_hrm.F_EXT   = 1
    shadow3_kb_hrm.AXMIN   = AXMIN
    shadow3_kb_hrm.AXMAJ   = axmaj_values[index]
    shadow3_kb_hrm.ELL_THE = ELL_THE   
        
    shadow3_beam.traceOE(shadow3_kb_hrm, kb_hrm_oe_number) # 
      
    # 2D DISTRIBUTION X,Z TO OBTAIN THE FOCAL SPOT DIMENSION 
    ticket2D = shadow3_beam.histo2(col_h=1, col_v=3, nbins=nbins, ref=23, xrange=[x_min, x_max], yrange=[z_min, z_max])
    
    histogram = ticket2D["histogram"]
    fwhms[index] = ticket2D["fwhm_v"]

    for ix in range(nbins):
        for iz in range(nbins):
             stack_result[index, ix, iz] = histogram[ix, iz]

    # FOCUS POSITION
    ticketFocus = ST.focnew(shadow3_beam, mode=0, center=[0.0, 0.0])    
    
    best_focus_positions[index] = ticketFocus['z_waist']
    
    print("AXMAJ, FWHM, Focus", shadow3_kb_hrm.AXMAJ, fwhms[index], best_focus_positions[index])
    

plot_data1D(axmaj_values, fwhms*1e4, "SPOT VERTICAL SIZE", "AXMAJ [cm]", "Z FWHM [um]")
plot_data1D(axmaj_values, best_focus_positions, "VERTICAL FOCUS RELATIVE POSITION", "AXMAJ [cm]", "Distance [cm]")
plot_data3D(stack_result, axmaj_values, numpy.linspace(x_min, x_max, nbins)*1e4, numpy.linspace(z_min, z_max, nbins)*1e4, "SPOT SIZE AT IMAGE PLANE", "X [um]", "Z [um]")


