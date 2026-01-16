import ansi
from PIL import Image
from docopt import docopt

import json
import base64
import io
import math
import shutil

__doc__ = """
Usage: jupytercli.py <notebook> [--height=h --width=w]

Options:
  -h --help             show this screen.
  --height=h            Force screen height [default: current term size]
  --width=w             Force screen width [default: current term size]
"""


def terminal_size():
    size = shutil.get_terminal_size((80, 20))
    return size.columns, size.lines


def format_output(output, screen_size):
    if output['output_type'] == "display_data":
        image_fd = io.BytesIO(base64.b64decode(output['data']['image/png']))
        img = Image.open(image_fd)
        img.thumbnail(screen_size, Image.ANTIALIAS)
        img_ansi = ansi.generate_ANSI_from_pixels(img.load(),
                                                  img.width,
                                                  img.height,
                                                  None,
                                                  is_overdraw=True)
        return '\n' + img_ansi[0] + '\x1b[39m\x1b[49m'
    elif output['output_type'] == "stream":
        return "".join(output['text']).strip()
    elif output['output_type'] == "execute_result":
        return "".join(output['data']['text/plain']).strip()

    return output


def parse_source(source, cellnum, padding=3):
    for line in source:
        line = line.rstrip()
        if not line:
            yield (padding+3)*' ' + ':'
        elif line[0].isspace():
            yield (padding+3)*'.' + ": " + line
        else:
            try:
                fstr = '[{: '+str(padding)+'d}]'
                cellstr = fstr.format(cellnum)
            except (ValueError, TypeError):
                cellstr = '[   ]'
            yield "{}: {}".format(cellstr, line)


def parse_notebook(notebookfd, screen_size=None):
    data = json.load(notebookfd)
    screen_size = screen_size or terminal_size()
    max_cellnum = max(c['execution_count'] or 1 for c in data['cells'])
    padding = int(math.log10(max_cellnum) or 1)
    for cell in data['cells']:
        cellnum = cell['execution_count']
        source = list(parse_source(cell['source'], cellnum, padding=padding))
        print("\n".join(source), end='\n\n')
        for output in cell['outputs']:
            print(format_output(output, screen_size), end='\n\n')
        print('-'*30, end='\n\n')


if __name__ == "__main__":
    dct = docopt(__doc__)
    notebook = dct['<notebook>']
    height = dct.get('height')
    width = dct.get('width')
    screen_size = (height, width)
    if not (height and width):
        screen_size = None
    with open(notebook) as fd:
        parse_notebook(fd, screen_size)