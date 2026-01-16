
import os
FREESURFER_HOME, SUBJECTS_DIR, SUBJECT, SUBJ_DIR, DMR, SEGMENT, ASEG_LIST, ASEG_LIST_LH_BS, ASEG_LIST_RH, SURF_VN, VOL_VN, SURF, LABEL, MRI= [os.environ[key] for key in 'FREESURFER_HOME SUBJECTS_DIR SUBJECT SUBJ_DIR DMR SEGMENT ASEG_LIST ASEG_LIST_LH_BS ASEG_LIST_RH SURF_VN VOL_VN SURF LABEL MRI'.split()]
#ASEG_LIST="8 10 11 12 13 16 17 18 26 47 49 50 51 52 53 54 58"
#ASEG_SURF="/Users/dionperd/CBR/VEP/JUNG/JUNG/aseg_surf"
import reconutils
import nibabel
import numpy as np

#vol_path=ASEG_SURF+"/aseg_filled.nii"
#out_vol_path=ASEG_SURF+"/aseg_filled_surf.nii"
#reconutils.vol_to_ext_surf_vol(vol_path,ASEG_LIST,out_vol_path=out_vol_path)
#
#if ASEG_SURF_METHOD=='gwi':
#    ASEG_GWI_VN, ASEG_GWI_THR = [os.environ[key] for key in 'ASEG_GWI_VN ASEG_GWI_THR '.split()]
#    vn=int(ASEG_GWI_VN)
#    th=float(ASEG_GWI_THR)
#    vol_path=ASEG_SURF+"/aseg_filled_surf.nii"
#    mask_vol_path=ASEG_SURF+"/gwi/gwi_mask.nii"
#    out_vol_path=ASEG_SURF+"/aseg_filled_surf_con_gwi.nii"  #
#    reconutils.mask_to_vol(vol_path,mask_vol_path,out_vol_path,ASEG_LIST,vol2maskTrnsfrmPath=None,vn=vn,th=1.0,labels_mask=ASEG_LIST,labels_nomask='0')
#elif ASEG_SURF_METHOD=='tdi':
#    ASEG_TDI_VN, ASEG_TDI_THR = [os.environ[key] for key in 'ASEG_TDI_VN ASEG_TDI_THR '.split()]
#    vn=int(ASEG_TDI_VN)
#    th=float(ASEG_TDI_THR)
#    vol_path=ASEG_SURF+"/aseg_filled_surf.nii"
#    mask_vol_path=ASEG_SURF+"/tdi/tdi_mask.nii"
#    out_vol_path=ASEG_SURF+"/aseg_filled_surf_con_tdi.nii"  #
#    vol2maskTrnsfrmPath=DMR+"/t2d.mat"
#    reconutils.mask_to_vol(vol_path,mask_vol_path,out_vol_path,ASEG_LIST,vol2maskTrnsfrmPath=vol2maskTrnsfrmPath,vn=vn,th=1.0,labels_mask=ASEG_LIST,labels_nomask='0')
#
#
#
#surf_path=ASEG_SURF+"./aseg_filled"
#vol_path=out_vol_path
#out_surf_path=os.path.splitext(out_vol_path)[0]
#reconutils.sample_vol_on_surf(surf_path,vol_path,out_surf_path,ASEG_LIST,vn=SURF_VN)
  
#lab1, ctab, names1 =nibabel.freesurfer.read_annot(SUBJ_DIR+'/label/lh.aparc.annot')
#(names_dict,colors)=reconutils.read_lut("/Applications/freesurfer/FreeSurferColorLUT.txt")
#(names2,ctab2)=reconutils.lut_to_annot_names_ctab("/Applications/freesurfer/FreeSurferColorLUT.txt",labels=["1","10"])

#reconutils.aseg_surf_conc_annot(SEGMENT+"/aseg_surfs/aseg",SEGMENT+"/lh.aseg",ASEG_LIST_LH_BS,lut_path=FREESURFER_HOME+"/FreeSurferColorLUT.txt")
#reconutils.aseg_surf_conc_annot(SEGMENT+"/aseg_surfs/aseg",SEGMENT+"/rh.aseg",ASEG_LIST_RH,FREESURFER_HOME+"/FreeSurferColorLUT.txt")

#reconutils.vol_to_ext_surf_vol(SEGMENT+'/aparc+aseg.nii',labels=ASEG_LIST,hemi='lh rh',out_vol_path=SEGMENT+'/aparc+aseg-surf.nii',labels_surf=None,labels_inner='0')
#
#reconutils.mask_to_vol(SEGMENT+'/aparc+aseg-surf.nii',SEGMENT+'/tdi/tdi_mask.nii',SEGMENT+'/aparc+aseg-surf-tdi.nii',labels=ASEG_LIST,hemi='lh rh',vol2maskTrnsfrmPath=SEGMENT+'/tdi/aparc+aseg2tdi.mat',vn=int(TDI_VN),th=1,labels_mask=None,labels_nomask='0')

#reconutils.sample_vol_on_surf(SEGMENT+'/aseg-surfs',SEGMENT+'/aparc+aseg-surf-tdi.nii',SEGMENT+'/aseg-surfs-tdi',labels=ASEG_LIST,hemi=None,
#                       lut_path=os.path.join(FREESURFER_HOME,'FreeSurferColorLUT.txt'),vn=1)
#                       
#reconutils.sample_vol_on_surf(SEGMENT+'/white-lh',SEGMENT+'/aparc+aseg-surf-tdi.nii',SEGMENT+'/white-lh-tdi',labels=None,hemi='lh',
#                       lut_path=os.path.join(FREESURFER_HOME,'FreeSurferColorLUT.txt'),vn=1)
#                       
#reconutils.sample_vol_on_surf(SEGMENT+'/white-rh',SEGMENT+'/aparc+aseg-surf-tdi.nii',SEGMENT+'/white-rh-tdi',labels=None,hemi='rh',
#                       lut_path=os.path.join(FREESURFER_HOME,'FreeSurferColorLUT.txt'),vn=1)                       

#reconutils.sample_vol_on_surf(SEGMENT+'/lh.aseg',SEGMENT+'/aparc+aseg-surf-tdi.nii',SEGMENT+'/lh.aseg-tdi-surf',annot_path=SEGMENT+'/lh.aseg.annot', vn=int(SURF_VN))
#reconutils.sample_vol_on_surf(SEGMENT+'/lh.white',SEGMENT+'/aparc+aseg-surf-tdi.nii',SEGMENT+'/lh.white-tdi-surf', ctx='lh',surf_ref_path=SEGMENT+'/lh.white-ras', out_surf_ref_path=SEGMENT+'/lh.white-ras-tdi-surf',vn=int(SURF_VN))
#reconutils.sample_vol_on_surf(SEGMENT+'/rh.white',SEGMENT+'/aparc+aseg-surf-tdi.nii',SEGMENT+'/rh.white-tdi-surf', ctx='rh',surf_ref_path=SEGMENT+'/rh.white-ras', out_surf_ref_path=SEGMENT+'/rh.white-ras-tdi-surf',vn=int(SURF_VN))

#reconutils.sample_vol_on_surf(SURF+'/rh.aseg',SEGMENT+'/aparc+aseg-mask.nii.gz',LABEL+'/rh.aseg.annot',SEGMENT+'/rh.aseg-mask',surf_ref_path=SURF+'/rh.aseg-ras',out_surf_ref_path=SEGMENT+'/rh.aseg-mask-ras',ctx=None,vn=int(SURF_VN))
#reconutils.sample_vol_on_surf(SURF+'/lh.aseg',SEGMENT+'/aparc+aseg-mask.nii.gz',LABEL+'/lh.aseg.annot',SEGMENT+'/lh.aseg-mask',surf_ref_path=SURF+'/lh.aseg-ras',out_surf_ref_path=SEGMENT+'/lh.aseg-mask-ras',ctx=None,vn=int(SURF_VN))


#volume = nibabel.load(SEGMENT+'/aparc+aseg.nii')
#...and get its data#vol = volume.get_data()
#lab, ctab, names=nibabel.freesurfer.read_annot(SUBJ_DIR+"/label/lh.aparc.annot") 

#(labels_dict,_,_)=reconutils.read_lut(key_mode='label')


#volume = nibabel.load(SEGMENT+'/aparc+aseg.nii')
##...and get its data
#vol = volume.get_data()
#volume_in_tdi = nibabel.load(SEGMENT+'/tdi/aparc+aseg-in-tdi.nii.gz')
##...and get its data
#voltdi = volume_in_tdi.get_data()
##...and its affine transform
#ijk2xyz_vol = volume.affine
##Read the mask volume...
#mask_vol = nibabel.load(SEGMENT+'/tdi/tdi_mask.nii')
##...and get its data
#mask = mask_vol.get_data()
#mask_shape=mask.shape
##...and invert its affine transform
#xyz2ijk_mask = np.linalg.inv(mask_vol.affine)
##If vol and mask are not in the same space:
##read the transform...
#vol2mask=np.loadtxt(SEGMENT+'/tdi/aparc+aseg2tdi.mat')
##...and apply it to the inverse mask affine transform:
#xyz2ijk_mask=np.dot(vol2mask,xyz2ijk_mask)
##Finally compute the transform from vol ijk to mask ijk:
#ijk2ijk=np.dot(ijk2xyz_vol,xyz2ijk_mask)
#
#ijk_vol=[128,128,128]
#print vol[ijk_vol[0],ijk_vol[1],ijk_vol[2]]
#ijk_mask=[60,97,42]
#print voltdi[ijk_mask[0],ijk_mask[1],ijk_mask[2]]
#xyz_vol=ijk2xyz_vol.dot(np.array([ijk_vol[0],ijk_vol[1],ijk_vol[2],1]))[:3]
#xyz_mask=mask_vol.affine.dot(np.array([ijk_mask[0],ijk_mask[1],ijk_mask[2],1]))[:3]
#xyz_vol2mask=vol2mask.dot(np.array([xyz_vol[0],xyz_vol[1],xyz_vol[2],1]))[:3]
#ijk_vol2mask=np.round(xyz2ijk_mask.dot(np.array([xyz_vol2mask[0],xyz_vol2mask[1],xyz_vol2mask[2],1]))[:3]).astype('i')#
#print voltdi[ijk_vol2mask[0],ijk_vol2mask[1],ijk_vol2mask[2]]
#ijkvol2ijkmask=np.round(ijk2ijk.dot(np.array([ijk_vol[0],ijk_vol[1],ijk_vol[2],1]))[:3]).astype('i')
#print voltdi[ijkvol2ijkmask[0],ijkvol2ijkmask[1],ijkvol2ijkmask[2]]

#reconutils.label_vol_from_tdi(SEGMENT+"/tdi/tdi_ends.nii",SEGMENT+"/tdi/tdi_lbl.nii")
#vol_path=SEGMENT+"/aparc+aseg.nii"
#node_vol_path=SEGMENT+'/tdi/tdi_lbl_v5.nii'
#con_mat_path=SEGMENT+'/tdi/aparc+aseg-counts5M-v5.csv'
#tract_length_path=SEGMENT+'/tdi/aparc+aseg-mean_tract_lengths5M-v5.csv'
#con_mat_path=SEGMENT+'/tdi/aparc+aseg-counts5M-v5.npy'
#reconutils.node_connectivity_similarity(con_mat_path,metric="cosine", mode='sim')
#reconutils.ijk2ijk_to_xyz2xyz(node_vol_path,vol_path,SEGMENT+'/tdi/tdi-to-aparc+aseg.mat',out_path='/tdi/tdi-to-aparc+aseg-xyz.mat')
#reconutils.ijk2ijk_to_xyz2xyz(SEGMENT+"/aparc+aseg.nii",SEGMENT+'/tdi/tdi_ends.nii',SEGMENT+'/tdi/aparc+aseg-to-tdi.mat',out_path=SEGMENT+'/tdi/aparc+aseg-to-tdi-xyz.mat')



#surf_path_in_ras=SEGMENT+"/lh.white-mask-ras"
#annot_path=SEGMENT+"/lh.white-mask.annot"
#ref_vol_path=DMR+'/tdi_lbl-v5.nii.gz'
#con_mat_path=DMR+'/vol-counts5M-v5.npy'
#parc_area=100.0
#labels=None
#hemi='lh'
#mode="con+geod+adj"
##t2d=SEGMENT+'/tdi/aparc+aseg-to-tdi-xyz.mat'
#lut_path=os.path.join(FREESURFER_HOME,'FreeSurferColorLUT.txt')
#
##Read the surface...
#(verts, faces,volume_info) = nibabel.freesurfer.io.read_geometry(surf_path_in_ras,read_metadata=True)
##...its annotation
#lab, ctab, names = nibabel.freesurfer.read_annot(annot_path)
##...and get the correspoding labels:
#labels_annot=reconutils.annot_names_to_labels(names,hemi,lut_path=lut_path)
##Set the target labels:
#(labels,nLbl)=reconutils.read_input_labels(labels=labels,hemi=hemi)
#if "con" in mode:  
#    #Compute voxel connectivity similarity matrix:
#    con=reconutils.node_connectivity_metric(con_mat_path,metric="cosine", mode='sim')
#    #Read the T1 to DTI transform: 
#    t2d=np.loadtxt(t2d)
#    #Read the reference tdi_lbl volume:
#    vollbl=nibabel.load(ref_vol_path)
#    vox=vollbl.get_data()
#    voxdim=vox.shape
#    #Get only the voxels that correspond to connectome nodes: 
#    voxijk,=np.where(vox.flatten()>0)
#    voxijk=np.unravel_index(voxijk, voxdim)
#    vox=vox[voxijk[0],voxijk[1],voxijk[2]]
#    #...and get their coordinates in xyz space
#    voxxzy=vollbl.affine.dot(np.c_[voxijk[0],voxijk[1],voxijk[2], np.ones(vox.shape[0])].T)[:3].T
#    del vollbl, voxijk
##Initialize the output:
#out_names=[]
#out_ctab=[]
#out_lab=lab
#nL=-1
#
#iL=1
#
##Find the corresponding label:
#lbl=labels_annot[iL]
##Get the mask of the respective vertices
#iV=lab==iL
#nV=sum(iV)
##If not enough vertices for at least one face are assgined to this label:
##if nV<3:
##    continue
###If it is not one of the target labels:
##if lbl not in labels:
##    #Just add this label to the new annotation as it stands:
##    out_names.append(names[iL])
##    out_ctab.append(ctab[iL])
##    #and change the output indices
##    nL+=1
##    out_lab[iV]=nL
##    continue
##Get the vertices and faces of this label:
#(verts_lbl,faces_lbl)=reconutils.extract_subsurf(verts,faces,iV)
##Calculate total area:
#roi_area=np.sum(reconutils.tri_area(verts_lbl[faces_lbl]))
##Calculate number of parcels:
#nParcs = int(np.round(roi_area/parc_area))
##If no further parcellation is needed
##if nParcs<2:
##    #Just add this label to the new annotation as it stands:
##    out_names.append(names[iL])
##    out_ctab.append(ctab[iL])
##    #and change the output indices
##    nL+=1
##    out_lab[iV]=nL
##    continue
#connectivity=None
#if ("adj" or "geod") in mode:
#    geodist=reconutils.vertex_connectivity(verts_lbl,faces_lbl,mode="sparse",metric='euclidean')
#    if "adj" in mode:
#        connectivity=geodist.copy()
#        connectivity.data=np.ones(connectivity.data.shape)
#        
#affinity=np.zeros((nV,nV))
#if "geod" in mode:  
#    #Compute geodesic distances
#    #geodist=gdist.local_gdist_matrix(verts_lbl, faces_lbl.astype('<i4'), max_distance=parc_area)
#    #Unpack sparse matrix
#    #geodist=geodist.todense()
#    from scipy.sparse.csgraph import shortest_path
#    #Set 0 values to maximum distance
#    geodist=shortest_path(geodist, method='auto', directed=False, return_predecessors=False, unweighted=False, overwrite=False)
#    max_gdist=np.max(geodist.flatten())
#    geodist[geodist==0]=max_gdist
#    #Set diagonal to 0
#    geodist=geodist-max_gdist*np.identity(nV)
#    #Convert them to normalized similarities
#    geodist=1-geodist/max_gdist
#    #...and add them to the affinity metric
#    affinity+=geodist
#    del geodist        
##    
##    
#from sklearn.cluster import AgglomerativeClustering    
#model = AgglomerativeClustering(n_clusters=nParcs-1, affinity="precomputed", connectivity=connectivity, linkage='average')
#model.fit(affinity) 
#resLbl=model.labels_    

#aaT1=nibabel.load(MRI+"/aparc+aseg.nii.gz")
#aaT1data=aaT1.get_data()
#affT1=aaT1.affine
#ijkT10=np.linalg.inv(affT1).dot(np.c_[0.0, 0.0, 0.0, 1].T)[:3].T
#
#aaD=nibabel.load(DMR+"/aparc+aseg-in-d.nii.gz")
#aaDdata=aaD.get_data()
#affD=aaD.affine
#ijkD0=np.linalg.inv(affD).dot(np.c_[0.0, 0.0, 0.0, 1].T)[:3].T
#
#t2d=np.loadtxt(DMR+"/t2d.mat")
#
#aaT12aaD=np.dot(np.linalg.inv(affT1),aaD.affine)
#aaT12aaD[:3,:][:,:3]=aaT12aaD[:3,:][:,:3]/2.0
surf_path=SURF+'/lh.white'
annot_path=LABEL+'/lh.aparc.annot'
con_verts_idx=SEGMENT+'/lh.white-mask-idx.npy'
parc_area=100
out_annot_path=SEGMENT+'/lh.white'+str(parc_area)+'.annot'
ref_vol_path=DMR+'/tdi_lbl-v5.nii.gz'
con_mat_path=DMR+'/vol-counts5M-v5.npy'
labels=None
hemi='lh'
mode="geod+adj" #con+
t2d= SEGMENT+'/aparc+aseg-to-tdilbl-v5.mat'
lut_path=os.path.join(FREESURFER_HOME,'FreeSurferColorLUT.txt')
reconutils.connectivity_geodesic_subparc(surf_path,annot_path,con_verts_idx,out_annot_path=annot_path,
                                  ref_vol_path=ref_vol_path,con_mat_path=con_mat_path,parc_area=parc_area,
                                  labels=labels,hemi=hemi, mode=mode, t2d=t2d, 
                                  lut_path=lut_path)