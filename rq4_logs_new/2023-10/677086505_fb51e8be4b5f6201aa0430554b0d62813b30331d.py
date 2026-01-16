from subprocess import call
from random import randint
from time import time

def create_ppt(markdown_text: str) -> bytes:
    identifier = str(int(time() * 1000)) + str((randint(0, 1000)))
    in_file = f'ppt/in/{identifier}.md'
    out_file = f'ppt/out/{identifier}.pptx'
    template_file = f'ppt/templates/template4.pptx'

    with open(in_file, 'w') as f:
        f.write(markdown_text)

    call(['pandoc', '-o', out_file, in_file, '--reference-doc', template_file])

    with open(out_file, 'rb') as f:
        return f.read()
    
    return ''