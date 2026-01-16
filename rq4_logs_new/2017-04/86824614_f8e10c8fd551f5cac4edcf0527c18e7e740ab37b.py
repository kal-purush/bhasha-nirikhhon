#!/usr/bin/python2
# -*- coding: utf-8 -*-

'''

TERA ... Ternary Phase Diagram Analyser
author: Merzuk Kaltak
website: http://github.com/mkaltak85/tera
last modified: August 2016

'''

import wx , sys 
from wx.lib.embeddedimage import PyEmbeddedImage
from basis import GetAppDir
import icons25 as ico
import icons45 as ico45
import widgets
from MyTools import LineToolForm
# output format numpy arrays 
from numpy import set_printoptions
float_formatter = lambda x: "%.4f" % x
set_printoptions( formatter={'float_kind':float_formatter}, linewidth =
        500 ) 
#
# main timer id 
#
ID_TIMER=1


###############################################################################
class MainFrame(wx.Frame): 
    def __init__(self, parent, title, *args, **kwargs):
        from matplotlib.backends.backend_wx import  wxc
        super(MainFrame, self).__init__(parent,*args, **kwargs) 
        #
        # set icon, which appears in taskbar 
        #
        self.SetIcon(ico45.Tera.GetIcon())
        #
        # App info set herer
        #
        self.info = wx.AboutDialogInfo()
        self.info.SetIcon(ico.Acronym.GetIcon())
        self.info.SetName('TERA')
        self.info.SetVersion('1.0')
        self.info.SetCopyright('(C) 2017 Merzuk Kaltak')
        self.info.SetWebSite('http://www.github.com/mkaltak85/tera')
        self.info.AddDeveloper('Merzuk Kaltak')
        self.info.AddDocWriter('Merzuk Kaltak')
        self.info.AddArtist('Tamara Pinterich')
        #
        # construct user interface 
        #
        self.InitUI()
        #
        # intialize timer
        #
        self.timer = wx.Timer(self, ID_TIMER)
        self.blick = 0 
        self.Bind(wx.EVT_TIMER, self.OnTimer, id=ID_TIMER)
        #
        # window size 
        #
        self.SetSize((1080, 850))
        if len(args)>0:
          self.SetTitle(args[0])
        #
        # show window 
        #
        self.Show(True)

        # 
        # open files if executed from terminal
        # 
        self.OpenArguments()

    # -------------------------------------------------------------------------
    # builds main user interface 
    # -------------------------------------------------------------------------
    def InitUI(self):
        self.MainPanel = wx.Panel(self)
        #DEBUG_BEGIN: show Standard Paths 
        # self.MainPanel = TestPanel(self) 
        #DEBUG_END: 
        font = wx.SystemSettings_GetFont(wx.SYS_SYSTEM_FONT)
        font.SetPointSize(9)
        # 
        # Create Menu and Toolbar bar with items: File, Help 
        # 
        menubar = wx.MenuBar()
        # File item contains: New, Open, Save, Close Tab, Close Application
        fileMenu = wx.Menu()
        fitem=fileMenu.Append(wx.ID_NEW, '&New', 'Create new diagram')
        self.Bind(wx.EVT_MENU, self.OnNew, fitem)
        fitem=fileMenu.Append(wx.ID_OPEN, '&Open', 'Open diagram')
        self.Bind(wx.EVT_MENU, self.OnOpen, fitem)
        fitem=fileMenu.Append(wx.ID_SAVE, '&Save' 'Save current diagram')
        self.Bind(wx.EVT_MENU, self.OnSave, fitem)
        fileMenu.AppendSeparator()
        qmi = wx.MenuItem(fileMenu, wx.ID_ANY, '&Close Tab\tCtrl+W')
        fileMenu.AppendItem(qmi)
        self.Bind(wx.EVT_MENU, self.OnQuitTab, qmi)
        qmi = wx.MenuItem(fileMenu, wx.ID_EXIT, '&Quit\tCtrl+Q')
        fileMenu.AppendItem(qmi)
        self.Bind(wx.EVT_MENU, self.OnQuit, qmi)
        menubar.Append(fileMenu, '&File')
        # View item contains: show status
        viewMenu = wx.Menu()
        self.shSet = viewMenu.Append(wx.ID_ANY, 'Show settings', 
            'Show Settings', kind=wx.ITEM_CHECK)
        self.shLog = viewMenu.Append(wx.ID_ANY, 'Show logfile', 
            'Show Log file', kind=wx.ITEM_CHECK)
        viewMenu.Check(self.shSet.GetId(), True)
        viewMenu.Check(self.shLog.GetId(), True)
        menubar.Append(viewMenu, '&View')
        # Help item contains: About
        helpMenu = wx.Menu()
        h=helpMenu.Append(wx.ID_HELP, '&Help')
        self.Bind(wx.EVT_MENU, self.OnHelpBox, h)
        about=helpMenu.Append(wx.ID_ABOUT, '&About')
        self.Bind(wx.EVT_MENU, self.OnAboutBox, about)
        menubar.Append(helpMenu, '&Help')
        self.SetMenuBar(menubar)
        # *********************************************************************
        vbox = wx.BoxSizer(wx.VERTICAL) # main box contains other horiz. boxes
        # *********************************************************************
        # 
        # Create toolbar 
        # 
        self.toolbar = widgets.CustomToolbar(self.MainPanel)
        vbox.Add(self.toolbar, 0, wx.EXPAND)
        # 
        # Split self.MainPanel into three parts A, B and C. 
        # 	A contains: Notebook with MPL canvas tabs
        #       B contains: Settings for current canvas
        #       C contains: Notebook with logger 
        # 
        self.topSplitter = widgets.ProportionalSplitter(self.MainPanel,
             id=-1, proportion = 0.66, style = wx.SP_BORDER) 
        self.vSplitter = widgets.ProportionalSplitter(self.topSplitter, 
             id=-1, proportion = 0.78, style = wx.SP_BORDER) 
        self.A = widgets.PanelA(self.vSplitter,self.toolbar)
        self.B = widgets.PanelB(self.vSplitter)
        self.vSplitter.SplitVertically(self.A, self.B)
        self.vSplitter.SetSashGravity(0.5)
        self.C = widgets.PanelC(self.topSplitter)
        self.topSplitter.SplitHorizontally(self.vSplitter, self.C)
        self.topSplitter.SetSashGravity(0.5)
        vbox.Add(self.topSplitter, 1, wx.EXPAND)
        self.Bind(wx.EVT_MENU, self.ToggleSettings, self.shSet)
        self.Bind(wx.EVT_MENU, self.ToggleLog, self.shLog)
        #
        # Add status bar with progress gauge 
        #  
        self.statusbar = widgets.ProgressStatusBar(self.MainPanel)
        vbox.Add(self.statusbar,0,wx.EXPAND)
        # *********************************************************************
        self.MainPanel.SetSizer(vbox) # finialize main vertical box 
        # *********************************************************************

    # -------------------------------------------------------------------------
    # Toggle Settings
    # -------------------------------------------------------------------------
    def ToggleSettings(self, e):
        x = self.MainPanel.GetSize()[0]
        if self.shSet.IsChecked():
           optx = int(x*0.78)
           self.vSplitter.SetSashPosition(optx)
        else:
           optx = int(x*1.0)
           self.vSplitter.SetSashPosition(optx)
        e.Skip()       

    # -------------------------------------------------------------------------
    # Toggle Settings
    # -------------------------------------------------------------------------
    def ToggleLog(self, e):
        ymax = self.MainPanel.GetSize()[1] - \
               self.toolbar.GetSize()[1] -\
               self.statusbar.GetSize()[1]
        if self.shLog.IsChecked():
           opty = int(ymax*0.66)
           self.topSplitter.SetSashPosition(opty)
        else:
           opty = int(ymax*1.0)
           self.topSplitter.SetSashPosition(opty)
        e.Skip()       

    # -------------------------------------------------------------------------
    # new tab
    # -------------------------------------------------------------------------
    def OnNew(self, e):
        #
        # open new diagram constructor dialog
        #
        text='Not implemented'
        dlg = wx.MessageDialog(self,text, 'Error',wx.OK|wx.ICON_ERROR)
        result = dlg.ShowModal()

    # -------------------------------------------------------------------------
    #  open arguments
    # -------------------------------------------------------------------------
    def OpenArguments(self):
        from dialog import ImportForm
	from poscar import MakeVESTA
        if len( sys.argv[1:] ) > 0 : 
           dlg = ImportForm(self,
           title='Import Data',
           style=wx.DEFAULT_FRAME_STYLE)
           dlg.CenterOnParent() 
           dlg.ShowModal()
           dlg.Destroy() 
           self.statusbar.SetRange( len(dlg.Projects) ) 
           i=0
           if len( dlg.Projects ) > 0 : 
              for project in dlg.Projects: 
                 self.A.NoteBook.AddTab(Project=project)
                 i+=1
                 self.statusbar.SetProgress( i ) 
           self.statusbar.prog.Hide()
           
    # -------------------------------------------------------------------------
    #  open 
    # -------------------------------------------------------------------------
    def OnOpen(self, e):
        from dialog import ImportForm
        dlg = ImportForm(self,
        title='Import Data',
        style=wx.DEFAULT_FRAME_STYLE)
        dlg.CenterOnParent() 
        dlg.ShowModal()
        dlg.Destroy() 
        self.statusbar.SetRange( len(dlg.Projects) ) 
        i=0
        if len( dlg.Projects ) > 0 : 
           for project in dlg.Projects: 
              self.A.NoteBook.AddTab(Project=project)
              i+=1
              self.statusbar.SetProgress( i ) 
        self.statusbar.prog.Hide()


    # -------------------------------------------------------------------------
    # Save project 
    # -------------------------------------------------------------------------
    def OnSave(self, e):
        from dialog import SaveProjectAs
        SaveProjectAs(self,self.A.NoteBook,self.C.PageLog,self.statusbar)

    # -------------------------------------------------------------------------
    # close tabs 
    # -------------------------------------------------------------------------
    def OnQuitTab(self, e):
        self.A.NoteBook.RemoveTab()
        e.Skip()

    # -------------------------------------------------------------------------
    # On Home 
    # -------------------------------------------------------------------------
    def OnHome(self, e):
        text='Home'
        callback='home'
        #       
        # use callback of MPL toolbar
        #       
        current_page = self.A.NoteBook.GetSelection()
        if current_page >= 0 : 
           SelectedMPLToolBar=self.A.NoteBook.GetPage(current_page).MPLToolBar
           getattr(SelectedMPLToolBar,callback)(e)
        e.Skip()

    # -------------------------------------------------------------------------
    # On Forward 
    # -------------------------------------------------------------------------
    def OnForward(self, e):
        text='Forward'
        callback='forward'
        #       
        # use callback of MPL toolbar
        #       
        current_page = self.A.NoteBook.GetSelection()
        if current_page >= 0 : 
           SelectedMPLToolBar=self.A.NoteBook.GetPage(current_page).MPLToolBar
           ID=getattr(SelectedMPLToolBar,text+'ID')
           Status=SelectedMPLToolBar.GetToolState(ID)
           getattr(SelectedMPLToolBar,callback)(e)
        e.Skip()

    # -------------------------------------------------------------------------
    # On Back
    # -------------------------------------------------------------------------
    def OnBack(self, e):
        text='Back'
        callback='back'
        #       
        # use callback of MPL toolbar
        #       
        current_page = self.A.NoteBook.GetSelection()
        if current_page >= 0 : 
           SelectedMPLToolBar=self.A.NoteBook.GetPage(current_page).MPLToolBar
           ID=getattr(SelectedMPLToolBar,text+'ID')
           Status=SelectedMPLToolBar.GetToolState(ID)
           getattr(SelectedMPLToolBar,callback)(e)
        e.Skip()

    # -------------------------------------------------------------------------
    # On Zoom
    # -------------------------------------------------------------------------
    def OnZoom(self, e):
        text='Zoom'
        callback='zoom'
        off=( ('Pan','OnMove') ,(None,None))
	self.SetButtonStatus(e,text, callback, switchoff=off)
        e.Skip()

    # -------------------------------------------------------------------------
    # On Move
    # -------------------------------------------------------------------------
    def OnMove(self, e):
        text='Pan'
        callback='pan'
        off=( ('Zoom','OnZoom'), (None,None))
	self.SetButtonStatus(e,text, callback, switchoff=off)
        e.Skip()

    # -------------------------------------------------------------------------
    # On Opt, find optimum plot layout including resizing and shifting colorbar
    # -------------------------------------------------------------------------
    def OnOpt(self, e):
        text='Opt'
        callback='OnOpt'
        #       
        # use callback of MPL toolbar
        #       
        current_page = self.A.NoteBook.GetSelection()
        if current_page >= 0 : 
           SelectedMPLToolBar=self.A.NoteBook.GetPage(current_page).MPLToolBar
           getattr(SelectedMPLToolBar,callback)(e)
        e.Skip()

    # -------------------------------------------------------------------------
    # On Points
    # -------------------------------------------------------------------------
    def OnPoints(self, e):
        text='Points'
        callback='OnPoints'
	self.SetButtonStatus(e,text, callback)
        e.Skip()

    # -------------------------------------------------------------------------
    # On Tie Lines
    # -------------------------------------------------------------------------
    def OnTieLines(self, e):
        text='TieLines'
        callback='OnTieLines'
	self.SetButtonStatus(e,text, callback)
        e.Skip()

    # -------------------------------------------------------------------------
    # On Grid
    # -------------------------------------------------------------------------
    def OnGrid(self, e):
        text='Grid'
        callback='OnGrid'
	self.SetButtonStatus(e,text, callback)
        e.Skip()

    # -------------------------------------------------------------------------
    # On Snap
    # -------------------------------------------------------------------------
    def OnSnap(self, e):
        text='Snap'
        callback='OnSnap'
        off=(('LineTool','OnLineTool'),(None,None))
	self.SetButtonStatus(e,text, callback, switchoff=off)
        e.Skip()

    # -------------------------------------------------------------------------
    # On Binding
    # -------------------------------------------------------------------------
    def OnBinding(self, e):
        text='Binding'
        callback='OnBinding'
        off=(('Stability','OnStability'),('Reaction','OnReaction'))
	self.SetButtonStatus(e,text, callback, switchoff=off)
        e.Skip()

    # -------------------------------------------------------------------------
    # On Stability
    # -------------------------------------------------------------------------
    def OnStability(self, e):
        text='Stability'
        callback='OnStability'
        off=(('Binding','OnBinding'),('Reaction','OnReaction'))
	self.SetButtonStatus(e,text, callback, switchoff=off)
        e.Skip()

    # -------------------------------------------------------------------------
    # On Reaction 
    # -------------------------------------------------------------------------
    def OnReaction(self, e):
        text='Reaction'
        callback='OnReaction'
        off=(('Binding','OnBinding'),('Stability','OnStability'))
	self.SetButtonStatus(e,text, callback, switchoff=off)
        e.Skip()

    # -------------------------------------------------------------------------
    # On LineTool
    # -------------------------------------------------------------------------
    def OnLineTool(self, e):
        text='LineTool'
        callback='OnLineTool'
        off=(('Snap','OnSnap'),(None,None))
	self.SetButtonStatus(e,text, callback, switchoff=off)
        e.Skip()

    # -------------------------------------------------------------------------
    # On Toggle3D
    # -------------------------------------------------------------------------
    def OnToggle3D(self, e):
        text='Toggle3D'
        callback='OnToggle3D'
	self.SetButtonStatus(e,text, callback)
        e.Skip()

    # -------------------------------------------------------------------------
    # On Toggle3D
    # -------------------------------------------------------------------------
    def SetButtonStatus( self, e, text, callback, switchoff=None ):
        #       
        # use callback of MPL toolbar
        #       
        current_page = self.A.NoteBook.GetSelection()
        if current_page >= 0 : 
           # 
           # switch off Stability and Reaction if it is selected
           # 
           if switchoff is not None:
             for typ, call in switchoff:
                if typ is None :
                    continue
                if self.toolbar.ID[typ][current_page]:
                   k=getattr(self,call)
                   k(e)
           SelectedMPLToolBar=self.A.NoteBook.GetPage(current_page).MPLToolBar
           ID=getattr(SelectedMPLToolBar,text+'ID')
           Status=SelectedMPLToolBar.GetToolState(ID)
           getattr(SelectedMPLToolBar,callback)(e)
           Status=self.toolbar.ID[text][current_page]
           self.toolbar.ID[text][current_page] = not Status
           t=getattr(self.toolbar,text)
           if self.toolbar.ID[text][current_page] : 
              color= wx.SystemSettings.GetColour(wx.SYS_COLOUR_BTNHILIGHT)
              t.SetBackgroundColour( color )
              t.Refresh()
           else:
              color= wx.SystemSettings.GetColour(wx.SYS_COLOUR_BACKGROUND)
              t.SetBackgroundColour( color )
              t.Refresh()

           # When Snap or Line tool selected other buttons are disabled
           buts=SelectedMPLToolBar.tools 
           if text == 'Snap' or text == 'LineTool':
               for but in buts: 
                   if but == text :
                       continue
                   toff=getattr(self.toolbar,but)

                   # switch off buttons and record the status 
                   if SelectedMPLToolBar.button_is_enabled[ but ] : 
                       toff.Disable()
                       SelectedMPLToolBar.button_is_enabled[ but ] = False 
                   else:
                       toff.Enable()
                       SelectedMPLToolBar.button_is_enabled[ but ] = True

           # When disable Snap and Linetool when 3D toggle on 
           if text == 'Toggle3D' :
               for but in buts: 
                   if but == 'Snap' or but == 'LineTool' :
                      toff=getattr(self.toolbar,but)
                      # switch off buttons and record the status 
                      if SelectedMPLToolBar.button_is_enabled[ but ] : 
                          toff.Disable()
                          SelectedMPLToolBar.button_is_enabled[ but ] = False 
                      else:
                          toff.Enable()
                          SelectedMPLToolBar.button_is_enabled[ but ] = True

    # -------------------------------------------------------------------------
    # close TERA
    # -------------------------------------------------------------------------
    def OnQuit(self, e):
        self.Close()
        e.Skip()

    # -------------------------------------------------------------------------
    # about box
    # -------------------------------------------------------------------------
    def OnAboutBox(self, e):
        from about import citing_text,licence_text       
        description = citing_text

        self.info.SetDescription(description)
        self.info.SetLicence(licence_text)
        info = wx.AboutBox(self.info)
        e.Skip()

    # -------------------------------------------------------------------------
    # help box
    # -------------------------------------------------------------------------
    def OnHelpBox(self, e):
        from Help import HelpDialog
        dlg = HelpDialog(self, -1,
			"Help", size=(610,610), 
			style=wx.DEFAULT_DIALOG_STYLE | wx.RESIZE_BORDER)

        dlg.Show()
        e.Skip()

    def OnTimer(self, e):
       self.blick = self.blick + 1
       if self.blick == 25:
           self.statusbar.SetBackgroundColour('#E0E2EB')
           self.statusbar.Refresh()
           self.timer.Stop()
           self.blick = 0
 
# -----------------------------------------------------------------------------
# Execute program here 
# -----------------------------------------------------------------------------
if __name__ == '__main__':
    import os.path
    from dialog import ReadWriteCfgFile
    app = wx.App()
    A = wx.GetApp()
    # StandardPaths will use the value of wx.App().GetAppName()
    # for some of the stnadard path components.  Let's set it to
    # something that makes that obvious for the demo.  In your own
    # apps you'll set it in to something more meaningfull for your
    # app in your OnInit, (or just let it default.)
    APP_NAME = "tera"
    A.SetAppName(APP_NAME)
    ReadWriteCfgFile()
    frame = MainFrame(None, title='Ternay Phase Diagram Analyzer')
    app.SetTopWindow(frame)
    app.MainLoop()
    sys.exit()