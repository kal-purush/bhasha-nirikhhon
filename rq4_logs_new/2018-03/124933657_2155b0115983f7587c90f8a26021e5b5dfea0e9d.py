import xraylib
from orangecontrib.shadow.widgets.gui.ow_automatic_element import AutomaticElement
from orangecontrib.shadow.util.shadow_objects import ShadowBeam,  ShadowSource

#########################################################
#
# This xraylib call prevents an anomalous behaviour of
# SHADOW due to some unexplained python problem (apparently on the C-binder).
# It happens sometimes after installing new non-pip libraries
# I don't know how and why, but this call to xraylib removes the
# problem
#
#########################################################

def fixWeirdShadowBug():
    if not AutomaticElement.is_weird_shadow_bug_fixed:
        try:
            xraylib.Refractive_Index_Re("LaB6", 10000, 4)
        except:
            pass

        AutomaticElement.is_weird_shadow_bug_fixed = True

# WEIRD MEMORY INITIALIZATION BY FORTRAN. JUST A FIX.
def fix_Intensity(polarization, beam_out):
    if polarization == 0:
        for index in range(0, len(beam_out._beam.rays)):
            beam_out._beam.rays[index, 15] = 0
            beam_out._beam.rays[index, 16] = 0
            beam_out._beam.rays[index, 17] = 0

################################################

fixWeirdShadowBug()

shadow_src = ShadowSource.create_src()

shadow_src.src.NPOINT = 50000    # number_of_rays
shadow_src.src.ISTAR1 = 23423423 # seed

shadow_src.src.FGRID = 0
shadow_src.src.IDO_VX = 0
shadow_src.src.IDO_VZ = 0
shadow_src.src.IDO_X_S = 0
shadow_src.src.IDO_Y_S = 0
shadow_src.src.IDO_Z_S = 0

shadow_src.src.FSOUR = 0 # spatial_type (point)
shadow_src.src.FDISTR = 1 # angular_distribution (flat)

shadow_src.src.HDIV1 = -1.0e-6
shadow_src.src.HDIV2 = 1.0e-6
shadow_src.src.VDIV1 = -1.0e-6
shadow_src.src.VDIV2 = 1.0e-6
    
shadow_src.src.FSOURCE_DEPTH = 1 # OFF

shadow_src.src.F_COLOR = 1 # single value
shadow_src.src.F_PHOT = 0 # eV , 1 Angstrom

shadow_src.src.PH1 = 399.8

shadow_src.src.F_POLAR = 1
shadow_src.src.F_COHER = 0
shadow_src.src.POL_ANGLE = 0.0
shadow_src.src.POL_DEG = 1.0

shadow_src.src.F_OPD = 1
shadow_src.src.F_BOUND_SOUR = 0

print("Running SHADOW")

write_begin_file = 0
write_start_file = 0
write_end_file = 0

beam_out = ShadowBeam.traceFromSource(shadow_src,
                                      write_begin_file=write_begin_file,
                                      write_start_file=write_start_file,
                                      write_end_file=write_end_file)

fix_Intensity(shadow_src.src.F_POLAR, beam_out)

out_object = beam_out