import sublime
import sublime_plugin
import os
import os.path as path
from User.preprocessdefines import PreProcessDefines
#------------------------------------------------------------------------------------
# Global vars
#------------------------------------------------------------------------------------

#------------------------------------------------------------------------------------
class pre_processCommand(sublime_plugin.TextCommand):
	#------------------------------------------------------------------------------------
	# ,edit
	def run(self, edit):
		contentTxtRegion = sublime.Region(0,self.view.size())
		linesTxtRegion   = self.view.split_by_newlines(contentTxtRegion)
		fullPathFile	 = str( self.view.file_name() )
		dirFile 		 = str( os.path.dirname( fullPathFile ) )
		print("fullPathFile\t["+fullPathFile+"] ["+str(len(fullPathFile))+"]")
		print("dirFile\t\t["+dirFile+"]")
		print("Number of lines ["+str(len(linesTxtRegion))+"]")
		newFile = self.window.new_file()
		#Pre-processing defines
		#preProcess = PreProcessDefines()
		#preProcess.load_list_defines("defines.h",dirFile)
		#preProcess.converter_code(fullPathFile,dirFile)
		#for line in linesTxtRegion:
			#print('['+str(line)+']')
			#self.view.replace(edit, contentTxtRegion, '['+ str(line)+"]")
		#self.view.insert(edit, 0, "Hello, World!")
	#------------------------------------------------------------------------------------
