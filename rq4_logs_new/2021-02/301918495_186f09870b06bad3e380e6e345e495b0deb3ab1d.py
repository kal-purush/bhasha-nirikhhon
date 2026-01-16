#!/usr/bin/python

import rospy
import time
import sys
import os
import struct
from sensor_msgs.msg import PointCloud



class Cloud2File:
    
    
    
    def __init__(self):
        
        rospy.init_node("cloud_to_file_node", anonymous=True)
        
        self.prefix = rospy.get_param("~prefix", "d435i")
        self.file_name = rospy.get_param("file_name", "/mnt/WD500/UFMG/DISSERTACAO/results/registration_cloud.pcd")
        
        rospy.Subscriber("/" + self.prefix + "/cloud/map", PointCloud, self.callback)
        
        self.stop  = False
        if raw_input("") == "y" and self.n > 0:
            self.save_file()
    
    
    
    def callback(self, msg):
        
        if not self.stop:
        
            self.n = len(msg.points)
            self.xyz = msg.points
            self.rgb = msg.channels
            
            sys.stdout.write("3: Cloud map with %d points subscribed. Save it now?\n" % self.n)
            sys.stdout.flush()
    
    
    
    def save_file(self):
        
        self.stop = True
    
        if os.path.exists(self.file_name):
            if raw_input("\nFile already exists, replace it? (y/n): ") == "y":
                os.remove(self.file_name)
            else:
                sys.stdout.write("\nNo changes made.\n\n")
                sys.stdout.flush()
                sys.exit()
        
        t = time.time()
        
        self.hex = [(int(255*self.rgb[0].values[k])<<16) + (int(255*self.rgb[1].values[k])<<8) + 
                    (int(255*self.rgb[2].values[k])) for k in range(self.n)]
        
        self.float = [struct.unpack("f", struct.pack("i", self.hex[k]))[0] for k in range(self.n)]
        
        self.f = open(self.file_name,"w")
        
        self.f.write("# .PCD v0.7 - Point Cloud Data file format\n")
        self.f.write("VERSION 0.7\n")
        self.f.write("FIELDS x y z rgb\n")
        self.f.write("SIZE 4 4 4 4\n")
        self.f.write("TYPE F F F F\n")
        self.f.write("COUNT 1 1 1 1\n")
        self.f.write("WIDTH 1\n")
        self.f.write("HEIGHT %s\n" % self.n)
        self.f.write("VIEWPOINT 0 0 0 1 0 0 0\n")
        self.f.write("POINTS %s\n" % self.n)
        self.f.write("DATA ascii")
        
        [self.f.write("\n%s %s %s %s" % (self.xyz[k].x, self.xyz[k].y, self.xyz[k].z, self.float[k])) for k in range(self.n)]
        
        self.f.close()
        
        sys.stdout.write("3: %d points written to file in %.0fms\n" % (self.n, 1000*(time.time()-t)))
        sys.stdout.flush()



if __name__ == "__main__":
    Cloud2File()