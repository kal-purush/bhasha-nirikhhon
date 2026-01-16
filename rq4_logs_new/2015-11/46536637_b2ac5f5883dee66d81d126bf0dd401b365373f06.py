
import timeline_objects as to

root_folder = 'tl_site'
timeline_name = 'root.html'

def make_site(root, outname):
     root_t = to.Timeline(root)
     root_t.save_html(outname)

if __name__ == '__main__':
    make_site(root_folder, timeline_name)
    