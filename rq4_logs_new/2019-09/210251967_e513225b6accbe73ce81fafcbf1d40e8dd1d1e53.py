
from matplotlib import pyplot as plt
from pathlib import Path
from pyart.core import Radar
from pyart.graph import RadarDisplay
from pyart.io import read_nexrad_archive


class NexradImage(object):
    @staticmethod
    def create_image_file(nexrad_file: Path, image_file: Path):
        radar: Radar = read_nexrad_archive(str(nexrad_file))
        display: RadarDisplay = RadarDisplay(radar)
        fig = plt.figure(figsize=(12, 10))

        ax = fig.add_subplot(111)
        display.plot('reflectivity', 0, title='NEXRAD Reflectivity',
                     vmin=-32, vmax=64, colorbar_label='', ax=ax)
        #display.plot_range_ring(radar.range['data'][-1]/1000., ax=ax)
        display.set_limits(xlim=(-500, 500), ylim=(-500, 500), ax=ax)
        plt.savefig(str(image_file), bbox_inches='tight')
