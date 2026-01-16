import numpy as np

#=================== K1
kx1 = 6.0
kz1 = 6.0
chi1 = 1.0j

#=================== K2
kx2 = 1.0
kz2 = 6.0
chi2 = -4.5

#=================== K3
kx3 = 7.0
kz3 = 12.0
chi3 = 0.83j

#=================== K1-STAR
kx1_star = 6.0
kz1_star = 8.0
chi1_star = 1.0j

#=================== K3-STAR
kx3_star = 7.0
kz3_star = 14.0
chi3_star = 0.83j


#=================== K4
kx4 = 0.3
kz4 = 3.0
chi4 = 0.3

#=================== K5
kx5 = 1.5
kz5 = 4.0
chi5 = 1.0

#=================== K6
kx6 = 2.1
kz6 = 5.0
chi6 = 3.0

#=================== K7
kx7 = 1.0
kz7 = 6.0
chi7 = 2.0



wavepackets={}
wavepackets['KA_x'] = np.array([kx2])
wavepackets['KA_z'] = np.array([kz2])
wavepackets['KA_a'] = np.array([chi1])



wavepackets['KB_x'] = np.array([kx2, kx1])
wavepackets['KB_z'] = np.array([kz2, kz1])
wavepackets['KB_a'] = np.array([chi2, chi1])



wavepackets['KC_x'] = np.array([kx2, kx1, kx3])
wavepackets['KC_z'] = np.array([kz2, kz1, kz3])
wavepackets['KC_a'] = np.array([chi2, chi1, chi3])



wavepackets['KD_x'] = np.array([kx2, kx1_star, kx3_star])
wavepackets['KD_z'] = np.array([kz2, kz1_star, kz3_star])
wavepackets['KD_a'] = np.array([chi2, chi1_star, chi3_star])



wavepackets['KE_x'] = np.array([kx4, kx5, kx6, kx7])
wavepackets['KE_z'] = np.array([kz4, kz5, kz6, kz7])
wavepackets['KE_a'] = np.array([chi4, chi5, chi6, chi7])