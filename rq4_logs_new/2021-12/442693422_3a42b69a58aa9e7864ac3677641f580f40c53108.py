from PyQt5.QtWidgets import QApplication, QMainWindow
import vtk
from PyQt5 import QtCore, QtGui, QtWidgets
from vtk.qt.QVTKRenderWindowInteractor import QVTKRenderWindowInteractor
from ui.Ui_testvtk import Ui_MainWindow

import open3d as o3d
import os
import numpy as np
import matplotlib.pyplot as plt
import datetime

import math
import sys
import pyzed.sl as sl

sys.path.append(os.path.abspath("."))
import pclpyd as pcl

def show3d(pointcloud):
    fronty = pointcloud[:, 2]
    frontx = pointcloud[:, 0]

    sidey = pointcloud[:, 2]
    sidez = pointcloud[:, 1]

    topx = pointcloud[:, 0]
    topz = pointcloud[:, 1]
    draw3d(frontx, fronty, sidez, sidey, topx, topz, s=0.1)

def draw3d(frontx, frontz, sidey, sidez, topx, topy, s=1):
    
    plt.subplot(2, 2, 1)
    plt.scatter(frontx, frontz, s=s)
    plt.title('front')
    plt.subplot(2, 2, 2)
    plt.scatter(sidey, sidez, s=s)
    plt.title('side')
    plt.subplot(2, 2, 3)
    plt.scatter(topx, topy, s=s)
    plt.title('top')
    plt.show()
# for file in os.listdir("./output"):
#     if '.pcd' in file:
#         pcd = o3d.io.read_point_cloud(os.path.join("output",file))
#         print(file+": ")
#         print(pcd)

# o3d.visualization.draw_geometries([pcd])

def gen_shuffled_dataset(cloudfile):
    
    pass


def hut3d(file_dir = "/media/violin/32FAD888FAD84A2D/Users/VioLin/Documents/GitHub/kunbo_team/datasets/3D"):
    sub_dirs = os.listdir(file_dir)
        
    cloudfiles = []
    for sub_dir in sub_dirs:
        files = os.listdir(os.path.abspath(os.path.join(file_dir,sub_dir)))
        for file in files:
            if file[-3:] == 'txt':
                cloudfiles.append(os.path.abspath(os.path.join(file_dir,sub_dir,file)))
    # for i, pcd_file in enumerate(pcd_lists):
    #     if i in skiplist:
    #         continue
    # pcd = o3d.io.read_point_cloud(os.path.join(pcd_dir, pcd_file))
    args = np.array([6,6])
    colormap = pcl.readPCLfile(cloudfiles,args)

    # plt.figure()

    for i,key in enumerate(colormap.keys()):
        pointcloud = colormap[key].astype(np.float64)
        
        show3d(pointcloud)
        # pointcloud = np.asarray(pcd.points)  # .astype(np.float64)
        #     pcd_face_points = pcl.abstract_cube(pointcloud, face_args)
        #
        #     pcd_face.points = o3d.utility.Vector3dVector(pcd_face_points)
        #     o3d.visualization.draw_geometries([pcd_face])

        #     print(i)

        # 从点云中提取cube
        face_args = np.array([-1000, 1000, -1000, 1000, 0, 2200,6])
        pcd_face_points = pcl.abstract_cube(pointcloud, face_args)

        show3d(pcd_face_points)

    pass

def my3dvision():
    starttime = datetime.datetime.now()
    pcd_dir = "../datasets/3dvisionpcl/.XYZ"
    # pcd_dir = "../datasets/Linemod_preprocessed/models"
    # pcd_file = "0001_1_1.pcd"
    
    # pcd_file = "4-4.xyz"
    # 2-5 4-5 4-6
    # xyz = []
    # with open(os.path.join(pcd_dir,pcd_file),'r') as f:
    #     content = f.read()
    #     contact = content.split('\n')
    #     for line in contact:
    #         if line == '' or line.isdigit():
    #             continue
    #         else:
    #             atom = line.split()
    #             if atom[0] == atom[1] and atom[1] == atom[2] and atom[2] == '0':
    #                 continue
    #             # xyzitem = {'symbol':atom[0], 'position':{"x":atom[1],"y":atom[2],"z":atom[3]}}
    #             xyzitem = np.array([atom[0],atom[1],atom[2]])
    #             xyz.append(xyzitem)
    #             pass
    files = ["4-4.xyz","4-5.xyz", "4-6.xyz"]
    # files = os.listdir(pcd_dir)
    cloudfiles = []
    for file in files:
        if file[-3:] == 'xyz':
            cloudfiles.append(os.path.abspath(os.path.join(pcd_dir,file)))

    args = np.array([6]).astype(np.float64)
    from plyfile import PlyData
    
    
    cloudmap = pcl.readPCLfile(cloudfiles,args)
    for key in cloudmap.keys():
    # def convertply2numpy(src):
    #     res = np.zeros([src['vertex'].data['x'].shape[0],6])
    #     for i in range (src['vertex'].data['x'].shape[0]):
    #         res[i,0] = src['vertex'].data['x'][i]
    #         res[i,1] = src['vertex'].data['y'][i]
    #         res[i,2] = src['vertex'].data['z'][i]
    #         res[i,3] = src['vertex'].data['red'][i]
    #         res[i,4] = src['vertex'].data['green'][i]
    #         res[i,5] = src['vertex'].data['blue'][i]
    #     return res
        
    # for cloudfile in cloudfiles:
        # cloudmap = PlyData.read(cloudfile)
        # cloudmap = convertply2numpy(cloudmap)
        xyz = cloudmap[key][:,0:3]
        rgb = cloudmap[key][:,3:6]
        xyz = np.array(xyz).astype(np.float64)
        face_args = np.array([-500, 500, -500, 500, 0, 1000])
        pointcloud = pcl.abstract_cube(xyz, face_args)
        # pointcloud = xyz
        pcd_face = o3d.geometry.PointCloud()
        pcd_face.points = o3d.utility.Vector3dVector(pointcloud)
        # pcd_face.colors = o3d.utility.Vector3dVector(rgb)
        o3d.visualization.draw_geometries([pcd_face])
        show3d(pointcloud)
    pass


def rotatest():
    ori = np.array([0, 0, 1])
    vec = np.array([-1, -1, 0])
    theta = 90
    rot = math.cos(theta*math.pi/180)*np.array([[1, 0, 0], [0, 1, 0], [0, 0, 1]])+(1-math.cos(theta*math.pi/180))*ori.reshape(
        ori.shape[0], 1) * ori+math.sin(theta*math.pi/180)*np.array([[0, -1*ori[2], ori[1]], [ori[2], 0, -1*ori[0]], [-1*ori[1], ori[0], 0]])
    print(vec @ rot)

def deal():
    data_dir = "C:/Users/VioLin/Documents/GitHub/kunbo_team/data/YCB_Video_Dataset/data_syn"
    floders = os.listdir(data_dir)
    for floder in floders:
        import shutil
        files = os.listdir(os.path.join(data_dir,floder))
        for file in files:
            shutil.move(os.path.join(data_dir,floder,file), data_dir)
    pass

class Mywindow(QtWidgets.QMainWindow, Ui_MainWindow):

    def __init__(self):
        super(Mywindow, self).__init__()
        self.setupUi(self)
        self.setWindowTitle('test_gui')

        self.frame = QtWidgets.QFrame()
        self.vtkWidget = QVTKRenderWindowInteractor(self.frame)
        self.formLayout.addWidget(self.vtkWidget)

        self.ren = vtk.vtkRenderer()
        self.vtkWidget.GetRenderWindow().AddRenderer(self.ren)
        self.iren = self.vtkWidget.GetRenderWindow().GetInteractor()

        # Create source
        source = vtk.vtkConeSource()
        source.SetCenter(0, 0, 0)
        source.SetRadius(0.1)

        source1 = vtk.vtkSphereSource()
        source1.SetCenter(0, 0, 0)
        source1.SetRadius(0.3)

        # Create a mapper
        mapper = vtk.vtkPolyDataMapper()
        mapper.SetInputConnection(source.GetOutputPort())

        mapper1 = vtk.vtkPolyDataMapper()
        mapper1.SetInputConnection(source1.GetOutputPort())

        # Create an actor
        actor = vtk.vtkActor()
        actor.SetMapper(mapper)

        actor1 = vtk.vtkActor()
        actor1.SetMapper(mapper1)

        self.ren.AddActor(actor)
        self.ren.AddActor(actor1)

        self.ren.ResetCamera()

        # self.frame.setLayout(self.formLayout)
        # self.setCentralWidget(self.frame)

        self.show()
        self.iren.Initialize()


if __name__ == "__main__":
    # deal()
    hut3d()
    # my3dvision()
    # app = QtWidgets.QApplication(sys.argv)
    # window = Mywindow()
    # window.show()
    # sys.exit(app.exec_())