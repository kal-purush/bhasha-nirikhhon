"""
module demonstrates the operations on files and directories using
the python library SHUTIL. It covers the important functions which
were generally used.
"""

import shutil

"""
copyfile will create a copy of the SRC and returns the new file created.
OUTPUT : D:/documents/pancard_copy.pdf
"""
src = "D:/documents/pancard.pdf"
dst = "D:/documents/pancard_copy.pdf"
#print(shutil.copyfile(src, dst))

"""
shutil.copy will create a copy of the file using the same filename if it has
mentioned.
OUTPUT : D:/documents/bank_doc\pancard.pdf
"""
dst = "D:/documents/bank_doc"
#print(shutil.copy(src, dst))

"""
shutil.copytree will create a copy of the directory which doesn't exist.
OUTPUT : D:/Automation_copy
"""
src = "D:/Automation"
dst = "D:/Automation_copy"
#print(shutil.copytree(src,dst))

"""
shutil.rmtree will delete entire directory
OUTPUT : None(if the deletion is success)
"""
path = "D:/Automation_copy"
#print(shutil.rmtree(path))

"""
shutil.move moves a file or directory to another location and returns destination
OUTPUT : D:/Automation_New
"""
src = "D:/Automation"
dst = "D:/Automation_New"
#print(shutil.move(src,dst))

"""
shutil.disk_usage gives the statistics of the following info in bytes:
OUTPUT : usage(total=365307097088, used=59445481472, free=305861615616)
"""
#print(shutil.disk_usage("D:/documents"))

"""
shutil.make_archive creates the archive in the following formats:
        - ('bztar', "bzip2'ed tar-file")
        - ('gztar', "gzip'ed tar-file")
        - ('tar', 'uncompressed tar file')
        - ('xztar', "xz'ed tar-file")
        - ('zip', 'ZIP file')

shutil.get_archive_formats() will show the supported formats as above.
shutil.unpack_archive(base_name, format, root_dir, base_dir) extracts the specified file in the supported format.
base_name - file name in which the archive is created.
format - format specifier.
root_dir - path of the directory in which base_dir is located.
base_dir - dir name for which archive needs to be created.

"""
print(shutil.make_archive("C:/SIP", "zip", "D:/", "SIP"))
#print(shutil.get_terminal_size())
print(shutil.unpack_archive("C:/SIP.zip", "C:/", "zip"))






























