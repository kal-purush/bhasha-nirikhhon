import unipath
import django.conf
from django.template.loader import render_to_string

pkg_path = unipath.Path(__file__).ancestor(2)
template_path = pkg_path.child('templates')
output_path = pkg_path.child('output', 'web', 'index.html')

django.conf.settings.TEMPLATE_DIRS = (django.conf.settings.TEMPLATE_DIRS +
                                      (template_path,))
data = { 'STATIC_URL': django.conf.settings.STATIC_URL }
output_path.write_file(render_to_string('alquraishi_sh2_networks/index.html', data))