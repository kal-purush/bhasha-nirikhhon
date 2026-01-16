#!/usr/bin/python
#################################################################################
#   Program for generating and viewing gcov files                               #
#   Workspace is choosen from menu bar                                          #
#   click on the button to genrate gcov files for selected workspace            #
#   Click on the name of a particular file from the tree to see it's content    #
#################################################################################

#Graficall support
import wx
#Directory acces support
import os.path, dircache
#Regular expressions support
import re
#Scrolling pannel support
import wx.lib.scrolledpanel
#System calls support
from subprocess import call

#support for reading .gcno files
#Choosing workspace via onChoose button won't work otherwise
import sys
reload(sys)
sys.setdefaultencoding("ISO-8859-1")

#Main frame that contains all other components
class MyFrame(wx.Frame):
    #Initialising the frame
    def __init__(self, parent, title):
            #Calling the constructor of the parent class
            super(MyFrame, self).__init__(parent, title=title, size=(1000, 600))
            #setting the frame in the centre of the screen
            self.Centre()
            #maiking a menu bar for workspace choosing and quitting
            menubar = wx.MenuBar()
            #making a menu (furure File in menu bar)
            fileMenu = wx.Menu()
            #adding items: Quit and Choose workspace in menu File
            fitem = fileMenu.Append(wx.ID_EXIT, 'Quit', 'Quit application')
            fitem2 = fileMenu.Append(1, 'Choose workspace', 'Chose workspace')
            #adding menu File in menu bar
            menubar.Append(fileMenu, '&File')
            #setting menu bar
            self.SetMenuBar(menubar)
            #binding the event: click on item quit with function OnQuit
            self.Bind(wx.EVT_MENU, self.OnQuit, fitem)
			#binding the event: click on item choose with function onChoose
            self.Bind(wx.EVT_MENU, self.onChoose, fitem2)
            #making frame visible
            self.Show()
            #setting the frame size to be screen size
            self.Maximize(True)
            #settng the initial value of workspace
            self.workspace = '/home'
            #making a spliiter in order to devide the frame
            self.splitter = wx.SplitterWindow(self, -1)
            #setting minimum size of future frame parts
            self.splitter.SetMinimumPaneSize(300)
            #defining left panel in left part of the frame
            leftPanel = wx.Panel(self.splitter, -1)
            #defining a sizer for left panel
            leftBox = wx.BoxSizer(wx.VERTICAL)
            #defining in left panel a tree structure that will hold the names of gcov files
            self.tree = wx.TreeCtrl(leftPanel, 1, wx.DefaultPosition, (-1, -1), wx.TR_HAS_BUTTONS)
            #binding the event: tree expanding with function onExpand
            wx.EVT_TREE_ITEM_EXPANDING(self.tree, self.tree.GetId(), self.onExpand)
            #binding the event: item selecting with function click
            self.Bind(wx.EVT_TREE_SEL_CHANGED, self.click)
            #setting the sizer in left panel
            leftPanel.SetSizer(leftBox)
            #defining right panel in right part of the frame
            rightPanel = wx.Panel(self.splitter, -1)
            #defining a sizer for right panel
            rightBox = wx.BoxSizer(wx.VERTICAL)
            #defining in right panel a text field that will hold the content of a choosen gcov file
            #option style is set so it can allow scrolling and some text showing posibillities
            self.display = wx.TextCtrl(rightPanel, -1, '', style = wx.TE_RICH | wx.TE_MULTILINE | wx.VSCROLL)
            #defining in left panel a button for generating gcov and function files
            self.but = wx.Button(leftPanel, 1, 'GENERATE GCOV AND FUNCTION FILES', size = (300,50))
            #binding the event: click on the button with function but_click
            self.but.Bind(wx.EVT_BUTTON, self.but_click)
            #adding components defined (tree, button and text field) in propper sizers
            rightBox.Add(self.display, -1, wx.EXPAND)
            leftBox.Add(self.but)
            leftBox.Add(self.tree, 1, wx.EXPAND)
            #setting the sizer in right panel
            rightPanel.SetSizer(rightBox)
            #splitting the frame on: left and right panel
            self.splitter.SplitVertically(leftPanel, rightPanel)

    #Function for workspace choosing.
    #It is called when menu item: Choose workspace is clicked
    #Arguments: component name and binding event (generic: self, event)
    #No return value
    def onChoose(self, event):
        #opening a directory choosing dialog
		dlg = wx.DirDialog(self, "Choose workspace")
        #if it is chosen...
		if dlg.ShowModal() == wx.ID_OK:
            #saving the path to the choosen directory in variable workspace
			self.workspace = dlg.GetPath()
			#deleting old tree
        	self.tree.DeleteAllItems()
        	#building new tree
        	self.buildTree(self.workspace)
        #closing the directory choosing dialog
		dlg.Destroy()

    #Function for generating gcov and fun files
    #It is called in function but_click
    #Arguments: component name and workspace name (generic: component name is self)
    #No return value
    def gcda(self, dir):
        #regular expression for gcda files (gcda-gcov data files)
        re_file = re.compile(r'(\w+)((.\w)?)(\.gcda)')
        #recursive search of all gcda files in workspace or it's subdirectories
        #calling gcov system call for each gcda file
        for f in os.listdir(dir):
            if os.path.isdir(dir+"/"+f):
                self.gcda(dir+"/"+f)
            elif re_file.match(f):
				file = open( dir + "/" + f[:-5] + ".gcno", 'r')
				string = file.read()
				if string.find(f[:-5]) > 0:
					folder_list = dir.split("/")
 					new_path = ""
					for folder in folder_list :
						if string.find(folder + "/") > 0 and re.search('[a-zA-Z]|[0-9]|_', folder):
							new_path = new_path + "/" + folder
					new_f = new_path + "/" +  f
					if new_path != "":
						if new_path.split("/")[1] != dir.split("/")[1]:
							new_dir = dir.split(new_path)[0]
							os.chdir(new_dir)
							os.system("gcov -f " + new_f[1:] + " > " + f[:-5] + ".fun")
						else:
							os.chdir(dir)
							os.system("gcov -f " + f + " > " + f[:-5] + ".fun")
					else:
						os.chdir(dir)
						os.system("gcov -f " + f + " > " + f[:-5] + ".fun")

    #Function for generating gcov and fun files and refreshing the tree
    #It is called when button Generate gcov and function files is clicked
    #Arguments: component name and binding event (generic: self, e)
    #No return value
    def but_click(self,e):
        #generating gcov files
        self.gcda(self.workspace)
        #deleting old tree
        self.tree.DeleteAllItems()
        #building new tree
        self.buildTree(self.workspace)

    #Function for quiting the program
    #It is called when menu item: Quit is clicked
    #Arguments: component name and binding event (generic: self, e)
    #No return value
    def OnQuit(self, e):
        self.Close()

    #Function for displaying the content of a gcov or fun file
    #It is called when an item is selected in the tree of names
    #Arguments: component name and binding event (generic: self, event)
    #No return value
    def click(self, event):
        #getting the name of selected file
        item = event.GetItem()
        #getting data about selected item
        itemData = self.tree.GetItemData(item).GetData()
        #getting path of selected item
        addr = itemData[0]
        #if it is a directory...
        if os.path.isdir(addr):
                    #reset the display
                    self.display.SetValue("")
        #if it is a file (then it is a gcov or fun file)
        else:
                    #open the selected file
                    file = open(addr, 'r')
                    #read the content of selectted file
                    string = file.read()
                    #formating the output
                    string2 = string.replace(' ', '   ')
                    string3 = string2.replace('   #', '#')
                    #write the content of the selected file in text field (a.k.a. display)
                    self.display.SetValue(string3)

    #Function for checking whether the node has been previously expanded.
    #If not, building out the node, which is then marked as expanded.
    #It is called when the user expands a node on the tree object.
    #Arguments: component name and binding event (generic: self, event)
    #No return value
    def onExpand(self, event):
        # getting the wxID of the entry to expand and checking it's validity
        itemID = event.GetItem()
        if not itemID.IsOk():
            itemID = self.tree.GetSelection()
        # only build that tree if not previously expanded
        old_pydata = self.tree.GetPyData(itemID)
        if old_pydata[1] == False:
            # clean the subtree and rebuild it
            self.tree.DeleteChildren(itemID)
            self.extendTree(itemID)
            self.tree.SetPyData(itemID,(old_pydata[0], True))

    #Function for building the tree of gcov and fun file names
    #Arguments: component name and workspace name (generic: component name is self)
    #No return value
    def buildTree(self, rootdir):
        #Add a new root element and then its children
        self.rootID = self.tree.AddRoot(rootdir)
        self.tree.SetPyData(self.rootID, (rootdir,1))
        self.extendTree(self.rootID)
        self.tree.Expand(self.rootID)

    #recursive search of files that match regular expression re_file in path or it's subdirectories
    #returns the number of hits
    #Arguments: component name, absolute path to the directory in wich we search,
    	#regular expression we try to match, variable that will store the number of found files
    #Return value is the number of files found
    def rekurzija(self, path, re_file,value):
            for f in os.listdir(path):
                    if os.path.isdir(path+"/"+f):
                            value = self.rekurzija(path+"/"+f, re_file, value)
                    elif re_file.match(f):
                            value+=1
            return value

    #Function for adding the children in the tree
    #A child is added if it is a gcov file, or directory in the absolute path of some gcov or fun file
    #Arguments: component name and workspace name (generic: component name is self)
    #No return value
    def extendTree(self, parentID):
        #setting root and regular expression for gcov and fun files
        parentDir = self.tree.GetPyData(parentID)[0]
        re_file = re.compile(r'(\w+)((.\w)?)((\.gcov)|(\.fun))')
        #recursive search
        subdirs = dircache.listdir(parentDir)
        subdirs.sort()
        for child in subdirs:
            child_path = os.path.join(parentDir,child)
            #it must be a directory in the absolute path of some gcov file...
            if os.path.isdir(child_path) and self.rekurzija(child_path, re_file, 0) and not os.path.islink(child):
                childID = self.tree.AppendItem(parentID, child)
                self.tree.SetPyData(childID, (child_path, False))
                newParent = child
                newParentID = childID
                newParentPath = child_path
                newsubdirs = dircache.listdir(newParentPath)
                newsubdirs.sort()
                for grandchild in newsubdirs:
                    grandchild_path = os.path.join(newParentPath,grandchild)
                    if os.path.isdir(grandchild_path) and self.rekurzija(child_path, re_file, 0) and not os.path.islink(grandchild_path):
                        grandchildID = self.tree.AppendItem(newParentID, grandchild)
                        self.tree.SetPyData(grandchildID, (grandchild_path,False))
                    elif re_file.match(grandchild):
                        grandchildID = self.tree.AppendItem(newParentID, grandchild)
                        self.tree.SetPyData(grandchildID, (grandchild_path,False))
            #or a gcov file itself
            elif re_file.match(child):
                childID = self.tree.AppendItem(parentID, child)
                self.tree.SetPyData(childID, (child_path, False))

#Main function
if __name__ == "__main__":
    #defining application
    app = wx.PySimpleApp(0)
    #initializing all available image handlers
    wx.InitAllImageHandlers()
    #defining the frame
    frame = MyFrame(None, "")
    #setting the frame
    app.SetTopWindow(frame)
    #making frame visible
    frame.Show()
    #starting the application (the program)
    app.MainLoop()