"""
Generates suitable resolution head surf and EEG/MEG sensors descriptions.

"""

import os
import os.path
import numpy as np
from nibabel.freesurfer import read_geometry


def mask_mesh(v, f, mask):
    "Apply vertex-wise mask to mesh."
    nv = v.shape[0]
    idx = np.r_[:nv][mask]
    faces = []
    for i, j, k in f:
        if (i in idx) and (j in idx) and (k in idx):
            faces.append([i, j, k])
    faces = np.array(faces)
    verts = v[mask]
    face_map = np.r_[:f.max() + 1]
    face_map[idx] = np.r_[:verts.shape[0]]
    faces = face_map[faces]
    return verts, faces


def write_off(fname, v, f, overwrite=False):
    if not os.path.exists(fname) or overwrite:
        nv = v.shape[0]
        nf = f.shape[0]
        with open(fname, 'w') as fd:
            fd.write('OFF\n%d %d 0\n' % (nv, nf))
            for x, y, z in v:
                fd.write('%f %f %f\n' % (x, y, z))
            for i, j, k in f:
                fd.write('3 %d %d %d\n' % (i, j, k))
        print('%s written' % (fname, ))
    else:
        print('%s exists, use overwrite=True to overwrite.' % (fname, ))


def remesh_off(remesher_path, ref_fname, out_fname, overwrite=False):
    "Run remesher on mesh in OFF format."
    if not os.path.exists(out_fname) or overwrite:
        os.system('%s %s %s' % (remesh_path, ref_fname, out_fname))
    else:
        print('%s exists, use overwrite=True to overwrite.' % (out_fname, ))


def read_off(fname):
    "Read mesh in from OFF format file."
    vl, fl = [], []
    with open(fname, 'r') as fd:
        fd.readline()
        nv, nf, _ = [int(i) for i in fd.readline().strip().split()]
        for _ in range(nv):
            vl.append([float(f) for f in fd.readline().strip().split()])
        for _ in range(nf):
            fl.append([int(i) for i in fd.readline().strip().split()[1:]])
    vl = np.array(vl)
    fl = np.array(fl)
    return vl, fl


def make_cap_mask(vl, a=0.8, s=1.3, thresh=-0.3):
    "Make vertex-wise mask for cap assuming RAS coordinates."
    nvl = (vl - vl.min(axis=0))/vl.ptp(axis=0)
    capmask = (nvl[:, 1]*a - nvl[:, 2]*s) < thresh
    return capmask


def xyz2rgb(vl):
    "Map XYZ coordinates to RGB space."
    nv = vl.shape[0]
    nvl = (vl - vl.min(axis=0))/vl.ptp(axis=0)
    vcrgb = np.c_[nvl, np.ones((nv, 1))]
    return vcrgb


def vertex_normals(v, f):
    vf = v[f]
    fn = np.cross(vf[:,1] - vf[:, 0], vf[:, 2] - vf[:, 0])
    vf = [set() for _ in range(len(v))]
    for i, fi in enumerate(f):
        for j in fi:
            vf[j].add(i)
    vn = np.zeros_like(v)
    for i, fi in enumerate(vf):
        fni = fn[list(fi)]
        norm = fni.sum(axis=0)
        norm /= np.sqrt((norm**2).sum())
        vn[i] = norm
    return vn


def sens_xyz_ori(v, f, l):
    vn = vertex_normals(v, f)
    pos = np.zeros((n_sens, 6))
    np.add.at(pos, l, np.c_[v, vn])
    return pos / np.bincount(l)[:, np.newaxis]


# config
seghead = 'lh.seghead'
ctx = 'lh.pial.fsaverage5'
ref_fname = 'head-ref.off'
low_fname = 'head-low.off'
plot = True
n_sens = 128
eeg_scl = 1.1
meg_scl = 1.3
eeg_fname = 'eeg.xyz'
meg_fname = 'meg.squid'

# read FS high-res head surf
vh, fh = read_geometry('lh.seghead')

# make new head surf with remesher
write_off(ref_fname, vh, fh)
remesh_path = '../scripts/remesher-mac/cmdremesher/cmdremesher'
remesh_off(remesh_path, ref_fname, low_fname)

# read new head surf
vl, fl = read_off(low_fname)

# mask for cap
vc, fc = mask_mesh(vl, fl, make_cap_mask(vl))

# cluster vertices on cap 128 ways
import scipy.cluster.vq
_, vcl = scipy.cluster.vq.kmeans2(vc, n_sens, minit='points', missing='raise')
assert np.unique(vcl).size == n_sens

# TODO label sensor by nearest aparc name

# on EEG cap and MEG helmet, avg pos and vtx norm to get sensor positions and normals
eegpo = sens_xyz_ori(vc * eeg_scl, fc, vcl)
megpo = sens_xyz_ori(vc * meg_scl, fc, vcl)

# write sensor files for OpenMEEG
np.savetxt(eeg_fname, eegpo[:3])
np.savetxt(meg_fname, megpo)

# visualize results
if plot:
    from pyqtgraph import mkQApp
    app = mkQApp()
    import pyqtgraph.opengl as gl
    gvw = gl.GLViewWidget()
    hs = gl.GLMeshItem(meshdata=gl.MeshData(vl, fl, vertexColors=xyz2rgb(vl)), drawEdges=True)
    cap = gl.GLMeshItem(meshdata=gl.MeshData(vc*eeg_scl, fc), color=(0, 0, 1.0, 1.0), drawEdges=True)
    hc = np.c_[np.random.rand(n_sens, 3), np.ones((n_sens, 1))][vcl]
    helmet = gl.GLMeshItem(meshdata=gl.MeshData(vc*meg_scl, fc, vertexColors=hc), #color=(1, 0, 0.0, 1.0),
            drawEdges=True)
    lh = gl.GLMeshItem(meshdata=gl.MeshData(*read_geometry(ctx)), drawEdges=True, drawFaces=False)
    for item in [hs, cap, helmet, lh]:
        gvw.addItem(item)
    for po in (megpo, eegpo):
        gvw.addItem(gl.GLScatterPlotItem(pos=po[:, :3], size=7))
        gvw.addItem(gl.GLScatterPlotItem(pos=po[:, :3] + 8*po[:, 3:], size=4, color=(1, 0, 0, 1)))
    gvw.show()
    gvw.readQImage().save('head_sensors.png')
    app.exec_()