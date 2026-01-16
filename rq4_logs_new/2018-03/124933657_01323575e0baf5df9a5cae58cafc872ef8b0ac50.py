import numpy
from srwlib import *
from silx.gui.plot import Plot2D, ImageView

numPer = 40 # Number of ID Periods (without counting for terminations
undPer = 0.1 # Period Length [m]
Kv = 1.92 # 400 eV 3rd harmonic
photon_energy = 370 #399.8 # 3rd harmonic

import scipy.constants as codata
def magnetic_field_from_K(K, period_length):
    return K * 2 * pi * codata.m_e * codata.c / (codata.e * period_length)


def createUndulator(Kv, undPer, numPer):
    #***********Undulator
    By = magnetic_field_from_K(Kv, undPer) #Peak Vertical field [T]
    print("By calculated: " + str(By) + " T")
    Bx = 0.0 #Peak Vertical field [T]

    phBy = 0 #Initial Phase of the Vertical field component
    sBy = -1 #Symmetry of the Vertical field component vs Longitudinal position
    xcID = 0 #Transverse Coordinates of Undulator Center [m]
    ycID = 0
    zcID = 0 #Longitudinal Coordinate of Undulator Center [m]

    und = SRWLMagFldU([SRWLMagFldH(1, 'h', Bx, phBy, sBy, 1), SRWLMagFldH(1, 'v', By, phBy, sBy, 1)], undPer, numPer) #Planar Undulator
    magFldCnt = SRWLMagFldC([und], array('d', [xcID]), array('d', [ycID]), array('d', [zcID])) #Container of all Field Elements

    return magFldCnt

def createElectronBeam(undPer, numPer):
    #***********Electron Beam
    elecBeam = SRWLPartBeam()

    elecBeam.partStatMom1.x = 0. #Initial Transverse Coordinates (initial Longitudinal Coordinate will be defined later on) [m]
    elecBeam.partStatMom1.y = 0. #-0.00025
    # Roughly ! check!
    elecBeam.partStatMom1.z = -0.5*undPer*(numPer + 8) #Initial Longitudinal Coordinate (set before the ID)
    elecBeam.partStatMom1.xp = 0. #Initial Relative Transverse Velocities
    elecBeam.partStatMom1.yp = 0.
    elecBeam.partStatMom1.gamma = 2./0.51099890221e-03 #Relative Energy

    elecBeam.Iavg = 0.32 #Average Current [A]

    #2nd order statistical moments
    elecBeam.arStatMom2[0] = (0.2529e-3)**2 #<(x-x0)^2>
    elecBeam.arStatMom2[1] = 0
    elecBeam.arStatMom2[2] = (0.02881e-3)**2 #<(x'-x'0)^2>
    elecBeam.arStatMom2[3] = (0.01844e-3)**2 #<(y-y0)^2>
    elecBeam.arStatMom2[4] = 0
    elecBeam.arStatMom2[5] = (5.235e-6)**2 #<(y'-y'0)^2>
    # energy spread
    elecBeam.arStatMom2[10] = (0.80e-03)**2 #<(E-E0)^2>/E0^2

    return elecBeam

def createInitialWavefrontMeshSourceDimension(elecBeam):
    #****************** Initial Wavefront
    wfr = SRWLWfr() #For intensity distribution at fixed photon energy
    wfr.allocate(1, 101, 101) #Numbers of points vs Photon Energy, Horizontal and Vertical Positions
    wfr.mesh.zStart = 10.0 #Longitudinal Position [m] from Center of Straight Section at which SR has to be calculated
    wfr.mesh.eStart = photon_energy #Initial Photon Energy [eV]
    wfr.mesh.eFin = wfr.mesh.eStart #Final Photon Energy [eV]

    #OC: it makes sense to choose the initial ranges equal or a bit larger that the first aperture of the beamline (not much larger)

    wfr.mesh.xStart = -0.5e-3 #-0.00015 #Initial Horizontal Position [m]
    wfr.mesh.xFin = -1 * wfr.mesh.xStart #0.00015 #Final Horizontal Position [m]
    wfr.mesh.yStart = -0.5e-3 #-0.00015 #Initial Vertical Position [m]
    wfr.mesh.yFin = -1 * wfr.mesh.yStart#0.00015 #Final Vertical Position [m]

    wfr.partBeam = elecBeam

    return wfr

def createInitialWavefrontMeshAngularDistribution(elecBeam):
    #****************** Initial Wavefront
    wfr = SRWLWfr() #For intensity distribution at fixed photon energy
    wfr.allocate(1, 201, 101) #Numbers of points vs Photon Energy, Horizontal and Vertical Positions
    wfr.mesh.zStart = 100.0 #Longitudinal Position [m] from Center of Straight Section at which SR has to be calculated
    wfr.mesh.eStart = photon_energy #Initial Photon Energy [eV]
    wfr.mesh.eFin = wfr.mesh.eStart #Final Photon Energy [eV]

    #OC: it makes sense to choose the initial ranges equal or a bit larger that the first aperture of the beamline (not much larger)

    wfr.mesh.xStart = -1e-2 #-0.00015 #Initial Horizontal Position [m]
    wfr.mesh.xFin = -1 * wfr.mesh.xStart #0.00015 #Final Horizontal Position [m]
    wfr.mesh.yStart = -0.5e-2 #-0.00015 #Initial Vertical Position [m]
    wfr.mesh.yFin = -1 * wfr.mesh.yStart#0.00015 #Final Vertical Position [m]

    wfr.partBeam = elecBeam

    return wfr

def createCalculationPrecisionSettings():
    #***********Precision Parameters for SR calculation
    meth = 1 #SR calculation method: 0- "manual", 1- "auto-undulator", 2- "auto-wiggler"
    relPrec = 0.01 #relative precision
    zStartInteg = 0 #longitudinal position to start integration (effective if < zEndInteg)
    zEndInteg = 0 #longitudinal position to finish integration (effective if > zStartInteg)
    npTraj = 100000 #Number of points for trajectory calculation
    useTermin = 1 #Use "terminating terms" (i.e. asymptotic expansions at zStartInteg and zEndInteg) or not (1 or 0 respectively)
    arPrecParSpec = [meth, relPrec, zStartInteg, zEndInteg, npTraj, useTermin, 0]

    return arPrecParSpec

def createBeamlineSourceDimension(wfr):
    #***************** Optical Elements and Propagation Parameters

    distSrcLens = wfr.mesh.zStart

    distLensImg = distSrcLens #Distance from lens to image plane
    focLength = distSrcLens*distLensImg/(distSrcLens + distLensImg)

    opLens = SRWLOptL(_Fx=focLength, _Fy=focLength) #Thin lens
    opDrift = SRWLOptD(distLensImg) #Drift space from lens to image plane

    #Propagation paramaters (SRW specific)
    #X-ray BM SR case: propagation without "auto-resizing"
    #and with semi-analytical treatment of the quadratic phase terms, to save memory
    ppLens =  [0, 0, 1., 0, 0, 1.0, 1.0, 1.0, 1.0, 0, 0, 0]
    ppDrift = [0, 0, 1., 1, 0, 1.5, 8.0, 0.5, 8.0, 0, 0, 0]

    #"Beamline" - Container of Optical Elements (together with the corresponding wavefront propagation instructions)

    optBL = SRWLOptC([opLens, opDrift],
                     [ppLens, ppDrift])

    return optBL

###################################################################

def transform_srw_array(arI, mesh):
    dim_x = mesh.nx
    dim_y = mesh.ny

    x_coordinates = numpy.linspace(mesh.xStart, mesh.xFin, dim_x)
    y_coordinates = numpy.linspace(mesh.yStart, mesh.yFin, dim_y)

    data = numpy.squeeze(arI)
    np_array = data.reshape((dim_y, dim_x))
    np_array = np_array.transpose()
 
    return x_coordinates, y_coordinates, np_array

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

###################################################################

magFldCnt = createUndulator(Kv, undPer, numPer)
elecBeam = createElectronBeam(undPer, numPer)
wfrAngDist = createInitialWavefrontMeshAngularDistribution(elecBeam)
wfrSouDim = createInitialWavefrontMeshSourceDimension(elecBeam)
optBLSouDim = createBeamlineSourceDimension(wfrSouDim)

arPrecParSpec = createCalculationPrecisionSettings()

# This is the convergence parameter. Higher is more accurate but slower!!
# 0.2 is used in the original example. But I think it should be higher. The calculation may then however need too much memory.
sampFactNxNyForProp = 1.0 #0.6 #sampling factor for adjusting nx, ny (effective if > 0)

if(srwl_uti_proc_is_master()):
    
    # 1 calculate intensity distribution ME convoluted for dimension size
    
    print('   Performing Initial Single-E Electric Field calculation ... ', end='')
    arPrecParSpec[6] = sampFactNxNyForProp #sampling factor for adjusting nx, ny (effective if > 0)
    srwl.CalcElecFieldSR(wfrSouDim, 0, magFldCnt, arPrecParSpec)
    print('done')
        
    print('   Simulating Electric Field Wavefront Propagation for Source Dimension ... ', end='')
    srwl.PropagElecField(wfrSouDim, optBLSouDim)
    print('done')

    print('   Extracting Intensity from the Propagated Electric Field for Dim Nat ... ', end='')
    arI = array('f', [0]*wfrSouDim.mesh.nx*wfrSouDim.mesh.ny) #"flat" 2D array to take intensity data
    srwl.CalcIntFromElecField(arI, wfrSouDim, 6, 1, 3, wfrSouDim.mesh.eStart, 0, 0)   
       
    srwl_uti_save_intens_ascii(arI, wfrSouDim.mesh, "intensity_source_dimension.dat")
    print('done')    
    
    x, z, intensity_source_dimension = transform_srw_array(arI, wfrSouDim.mesh)    
       
    # 2 calculate intensity distribution ME convoluted at far field to express it in angular coordinates 

    print('   Performing Initial Single-E Electric Field calculation ... ', end='')
    arPrecParSpec[6] = sampFactNxNyForProp #sampling factor for adjusting nx, ny (effective if > 0)
    srwl.CalcElecFieldSR(wfrAngDist, 0, magFldCnt, arPrecParSpec)
    print('done')

    print('   Extracting Intensity ME Conv from the Calculated Initial Electric Field ... ', end='')
    arI = array('f', [0]*wfrAngDist.mesh.nx*wfrAngDist.mesh.ny) #"flat" array to take 2D intensity data
    srwl.CalcIntFromElecField(arI, wfrAngDist, 6, 1, 3, wfrAngDist.mesh.eStart, 0, 0)
    print('done')
    print('   Saving the Initial Wavefront Intensity into a file ... ', end='')
    
    wfrAngDist.mesh.xStart /= wfrAngDist.mesh.zStart
    wfrAngDist.mesh.xFin /= wfrAngDist.mesh.zStart
    wfrAngDist.mesh.yStart /= wfrAngDist.mesh.zStart
    wfrAngDist.mesh.yFin /= wfrAngDist.mesh.zStart
    
    srwl_uti_save_intens_ascii(arI, wfrAngDist.mesh, "intensity_angular_distribution.dat")
    print('done')    
    
    x_first, z_first, intensity_angular_distribution = transform_srw_array(arI, wfrAngDist.mesh)    

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
        
out_object = output
    