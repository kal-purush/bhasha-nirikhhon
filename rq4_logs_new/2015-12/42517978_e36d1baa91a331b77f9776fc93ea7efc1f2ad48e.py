
# --- 1

import numpy as np
import unittest

from openmdao.api import Problem, Group

from PGL.main.distfunc import distfunc
from fusedwind.turbine.geometry import read_blade_planform,\
                                       redistribute_planform,\
                                       PGLLoftedBladeSurface,\
                                       PGLRedistributedPlanform, \
                                       SplinedBladePlanform



def configure_surface():

    p = Problem(root=Group())
    root = p.root

    pf = read_blade_planform('data/DTU_10MW_RWT_blade_axis_prebend.dat')

    nsec_ae = 30
    nsec_st = 20
    dist = np.array([[0., 1./nsec_ae, 1], [1., 1./nsec_ae/3., nsec_ae]])
    s_ae = distfunc(dist)
    s_st = np.linspace(0, 1, nsec_st)
    pf = redistribute_planform(pf, s=s_ae)

    # --- 2

    # add planform spline component
    spl_ae = root.add('pf_splines', SplinedBladePlanform(pf), promotes=['*'])

    # component for interpolating planform onto structural mesh
    redist = root.add('pf_st', PGLRedistributedPlanform('_st', nsec_ae, s_st), promotes=['*'])

    # --- 3

    # configure blade surface
    cfg = {}
    cfg['redistribute_flag'] = False
    cfg['blend_var'] = np.array([0.241, 0.301, 0.36, 0.48, 1.0])
    afs = []
    for f in ['data/ffaw3241.dat',
              'data/ffaw3301.dat',
              'data/ffaw3360.dat',
              'data/ffaw3480.dat',
              'data/cylinder.dat']:

        afs.append(np.loadtxt(f))
    cfg['base_airfoils'] = afs
    surf = root.add('blade_surf', PGLLoftedBladeSurface(cfg, size_in=nsec_st,
                                    size_out=(200, nsec_st, 3), suffix='_st'), promotes=['*'])

    # --- 4

    # add splines to selected params
    for name in ['x', 'chord', 'rot_z', 'rthick']:
        spl_ae.add_spline(name, np.array([0, 0.25, 0.75, 1.]), spline_type='bezier')

    # configure
    spl_ae.configure()
    return p

    # --- 5

if __name__ == '__main__':

    p = configure_surface()
    p.setup()
    p.run()
    import matplotlib.pylab as plt
    plt.rc('lines', linewidth=3)

    plt.figure()
    plt.axis('equal')
    for i in range(p['blade_surface_st'].shape[1]):
        plt.plot(p['blade_surface_st'][:,i, 2], p['blade_surface_st'][:,i, 0], 'r-')
    plt.plot(p['blade_surface_st'][0, :, 2], p['blade_surface_st'][100, :, 0],'r-')
    plt.plot(p['blade_surface_st'][0, :, 2], p['blade_surface_st'][0, :, 0],'r-')
    plt.plot(p['z'], p['x'], 'r-', label='Original')

    # --- 6

    # add sweep to the blade
    p['x_C'][-1]=-.05
    p.run()
    for i in range(p['blade_surface_st'].shape[1]):
        plt.plot(p['blade_surface_st'][:,i, 2], p['blade_surface_st'][:,i, 0], 'b-')
    plt.plot(p['z'], p['x'], 'b-', label='Swept')
    plt.plot(p['blade_surface_st'][0, :, 2], p['blade_surface_st'][100, :, 0],'b-')
    plt.plot(p['blade_surface_st'][0, :, 2], p['blade_surface_st'][0, :, 0],'b-')
    plt.plot(p.root.pf_splines.x_s.Cx, p['x_C'], 'g--o', label='Spline CPs')
    plt.legend(loc='best')
    plt.savefig('bladesurface_topview.png')
    plt.show()

    # --- 7

    # modify chord
    plt.figure()
    plt.plot(p['s'], p['chord'], 'r-', label='Original')
    p['chord_C'][1]=.01
    p['chord_C'][2]=-.01
    p.run()
    plt.plot(p['z'], p['chord'], 'b-', label='Modified')
    plt.plot(p.root.pf_splines.chord_s.Cx, p['chord_C'], 'g--o', label='Spline CPs')
    plt.plot(p.root.pf_splines.chord_s.s, p.root.pf_splines.chord_s._P, 'm--o', label='')
    plt.legend(loc='best')
    plt.savefig('chord_ffd_spline.png')
    plt.savefig('chord_ffd_spline.eps')
    plt.show()



    # --- 8