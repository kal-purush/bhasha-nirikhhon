__author__ = 'Sergey Tomin'
from ocelot import *
from ocelot.gui.accelerator import *
from ocelot.adaptors import *

# injector
from I1 import *
method = MethodTM()
method.global_method = SecondTM #TransferMap
lattice=gun_5MeV + i1_150M
#lattice=test01
lat = MagneticLattice(lattice, method=method)
#lat = MagneticLattice(gun_5MeV, method=method)
tws = twiss(lat, tws_5M, nPoints=None)
print("'GUN': length = ", lat.totalLen, "s = ", tws[-1].s)

#PartFile='xcode.ast'
PartFile='D:\\XTRACK\\ASTRA\\XFEL_with_astra\\to14.44\\Exfel.0320.ast'
p_array, charge_array = astraBeam2particleArray(filename=PartFile)
s_start=3.2;
p_array.s = 0.

method = MethodTM()
method.global_method = SecondTM
phi1=18.7268
V1=18.455*1e-3/np.cos(phi1*pi/180)*1.0033
c_a1_1_1_i1.v = V1; c_a1_1_1_i1.phi = phi1
c_a1_1_2_i1.v = V1; c_a1_1_2_i1.phi = phi1
c_a1_1_3_i1.v = V1; c_a1_1_3_i1.phi = phi1
c_a1_1_4_i1.v = V1; c_a1_1_4_i1.phi = phi1
c_a1_1_5_i1.v = V1; c_a1_1_5_i1.phi = phi1
c_a1_1_6_i1.v = V1; c_a1_1_6_i1.phi = phi1
c_a1_1_7_i1.v = V1; c_a1_1_7_i1.phi = phi1
c_a1_1_8_i1.v = V1; c_a1_1_8_i1.phi = phi1

lat = MagneticLattice(lattice, start=start_sim, method=method)
lat.totalLen=14.25-s_start; #14.25

sc1 = SpaceCharge()
sc1.step = 1
sc1.nmesh_xyz = [63, 63, 63]
sc1.low_order_kick = True

wake1 = Wake()
wake1.wake_file = 'TESLA_MODULE_WAKE_TAYLOR.dat'
wake1.step = 1
wake1.factor = 1./11.0688

wake2 = Wake()
wake2.wake_file = 'THIRD_HARMONIC_SECTION_WAKE_TAYLOR.dat'
wake2.step = 1
wake2.factor = 1./4.864

navi = Navigator(lat)

navi.add_physics_proc(sc1, lat.sequence[0], lat.sequence[-1])
#navi.add_physics_proc(wake1, w_acc1_start, w_acc1_stop)
#navi.add_physics_proc(wake2, w_acc39_start, w_acc39_stop)

navi.unit_step = 0.025

tws_track, p_array = track(lat, p_array, navi)


p_array.s=p_array.s+s_start;
particleArray2astraBeam(p_array,charge_array, 'd:\\ocelot_sc2.ast')



plot_opt_func(lat, tws_track, top_plot=["E"], fig_name=0, legend=False)

sI1, I1 = get_current(p_array, charge=charge_array[0], num_bins=200)
tau = p_array.particles[4::6]
dp = p_array.particles[5::6]
x = p_array.particles[0::6]
xp = p_array.particles[1::6]
y = p_array.particles[2::6]
yp = p_array.particles[3::6]

#output
s_p=[t.s for t in tws_track]
n_out=len(s_p)
out=np.zeros((n_out,6))
out[:,0]=s_p
out[:,1]=[t.beta_x for t in tws_track]
out[:,2]=[t.beta_y for t in tws_track]
out[:,3]=[t.emit_x for t in tws_track]
out[:,4]=[t.emit_y for t in tws_track]
out[:,5]=[t.E for t in tws_track]
np.savetxt('D:/pyoptics_sc2.txt',out)


plt.figure(2)
plt.plot(-tau*1000, dp, 'r.')
plt.xlabel("s, mm")
plt.ylabel("dp/p")
plt.grid(True)

plt.figure(3)
plt.title("current: end")
plt.plot(sI1*1000, I1)
plt.xlabel("s, mm")
plt.ylabel("I, A")
plt.grid(True)
plt.show()