import django; django.setup()

import subprocess
from luctor import settings
from recipes.models import Lesson
import os




def get_text(fn):
    cmd = ["antiword", "-f", fn]
    error = []
    p = subprocess.Popen(cmd, stderr=subprocess.PIPE, stdout=subprocess.PIPE)
    out, err = p.communicate()
    encoding = "utf-8"
    if p.returncode == 0:
        return True, out.decode("utf-8")

    error.append("antiword: {err}".format(err=err.decode("latin-1")))
    
    if b'Rich Text Format' in err:
        cmd = ['/usr/bin/python3', '/usr/bin/unoconv', '-f', 'text', '--stdout', fn] 
        #cmd = ['unrtf', '--text', fn]
        p = subprocess.Popen(cmd, stderr=subprocess.PIPE, stdout=subprocess.PIPE)
        out, err = p.communicate()
        if p.returncode == 0:
            return True, out.decode("utf-8")
        
        #encoding = "latin-1"
        #if p.returncode == 0:
        #    out = out.decode("latin-1")
        #    out = out.split("-----------------", 1)[1].strip()
        #    return True, out

        error.append("unoconv: {err}".format(err=err.decode("latin-1")))
        
    cmd = ["odt2txt", "--encoding=utf-8", fn]
    p = subprocess.Popen(cmd, stderr=subprocess.PIPE, stdout=subprocess.PIPE)
    out, err = p.communicate()
    if p.returncode == 0:
        return True, out.decode("utf-8")
    
    error.append("odt2txt: {err}".format(err=err.decode("latin-1")))
    return False, "\n".join(error)
                
                     
            

for l in Lesson.objects.filter(status=1):
    fn = l.docfile.file.file.name
    print(l.id, fn)

    success, msg = get_text(fn)

    if success:
        l.status = 1
        l.raw_text = msg
        l.problems = ""
    else:
        l.problems = msg
    l.save()
