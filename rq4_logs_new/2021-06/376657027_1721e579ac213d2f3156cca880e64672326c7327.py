import numpy as np
import pandas as pd
import parse
import xml.etree.ElementTree as ET
import matplotlib.pyplot as plt
import sys


# body_name: [best_iso, pixels_w]
# number of pixels normalized to full frame size
body_metadata = {
    # "rx1007":  [ "1600", 5472/13.2*35.7],
    # "rx1005":  [ "1600", 5472/13.2*35.7],
    "a7r4":    [ "6400", 9504],
    # "a6400":   [ "6400", 6000/23.5*35.7],
    # "a6500":   [ "6400", 6000/23.5*35.7],
    "a6300":   [ "6400", 6000/23.5*35.7],
    # "a5100":   [ "6400", 6000/23.5*35.7],
    # "a5000":   [ "1600", 5000/23.5*35.7],
    # "a6000":   [ "6400", 6000/23.5*35.7],
    # "nex7":    [ "1600", 6000/23.5*35.7], # <
    # "nex6":    [ "3200", 4912/23.5*35.7], # <
    # "nex5n":   [ "1600", 4912/23.5*35.7],
    # "nex3n":   [ "1600", 4912/23.5*35.7],
    "nex5t":   [ "1600", 4912/23.5*35.7],
    # "a7r":     [ "1600", 7952],
    "a7r2":    [ "6400", 7952],
    "a7r3":    [ "6400", 7952],
    # "a7" :     [ "1600", 6000],
    # "a9" :     [ "3200", 6000],
    # "a92" :    [ "3200", 6000],
    "a73":     [ "6400", 6000],
    "a7s":     ["12800", 4240],
    # "a7s2":    ["12800", 4240],
    # "x1d-50c": [ "3200", 8272/43.8*35.7],
    # "645z":    ["12800", 8268/43.8*35.7],
    # "eosm6":   [ "3200", 6000/22.3*35.7],
    # "eosm10":  [ "6400", 5184/22.3*35.7],
    # "650d":    [ "3200", 5184/22.3*35.7],
    # "600d":    [ "3200", 5184/22.3*35.7], # > 650d
    # "eosm100": [ "6400", 6000/22.3*35.7], # > 600d
    # "eosrp":   [ "3200", 6240],
    # "eosr":    [ "6400", 6720], # = 5d4
    # "1dx2":     [ "6400", 5472],
    "1dx3":     [ "6400", 5472],
    # "5d4":     [ "6400", 6720],
    # "5d3":     [ "3200", 5760],
    # "5d2":     [ "1600", 5634],
    # "5dsr":    [ "6400", 8688],
    # "6d":      [ "6400", 5472],
    # "7d2":     [ "3200", 5472],
    # "d750":    [ "6400", 6016],
    "d850":    [ "6400", 8256],
    "d810":    [ "1600", 7360],
    "d800":    [ "1600", 7360],
    # "z7":      [ "6400", 8256],
    # "z6":      [ "6400", 6048],
    # "d7500":   [ "6400", 5568/23.5*35.7],
    "d5300":   [ "1600", 6000/23.5*35.7],
}

import itertools
marker_pool = itertools.cycle((',', '+', '.', 'o', '*', 'v', '1', '+', 'x', 'd', 's')) 

all_ISO = ["64"] + list(map(lambda x: 100 * 2 ** x, range(10)))

class CameraBody():
    template = parse.compile("Manufacturer ISO : {}<BR>Gray scale : {} %<BR><B>SNR : {} dB</B>")
    min_grey = 100
    min_snr = 100

    def __init__(self, body_name: str, best_iso: str, pixel_per_width: float):
        self._body_name = body_name
        self._best_iso = best_iso
        self._pixel_per_width = pixel_per_width
        self._load()

    def _load(self):
        self._iso_snr_curve = dict()
        # TODO: if I self. before body name it still works..
        root = ET.parse("{}.txt".format(self._body_name)).getroot()
        # each `dataSet' contains one curve for specific ISO
        for ds in root.findall("dataSet"):
            iso_name = ds.get("seriesName")
            this_iso_curve_points = []
            # (grey_scale_val, SNR)
            for pt in ds.findall("set"):
                res_str = CameraBody.template.parse(pt.get("toolText")).fixed
                iso, gray_scale, snr = list(map(float, res_str))
                this_point = self._normalize(iso, gray_scale, snr)
                this_iso_curve_points.append(this_point)

            # n*2 matrix
            self._iso_snr_curve[iso_name] = np.array(this_iso_curve_points)

    def __repr__(self):
        return self._body_name

    def get_curve(self, iso_name: str):
        if iso_name in self._iso_snr_curve:
            return self._iso_snr_curve[iso_name]
        else:
            return None
            
    def _normalize(self, iso: float, gray_scale: float, snr: float) -> (float, float):
        # input light has same density. But different ISO requires different exposure time.

        # assume the amount of exposure time is fixed.
        # assume the gray_scale value from dataset is the actual output gray scale value. e.g. 100% is just 12/14/16-bit all ones'.
        # then for given exposure time, to obtain the same gray scale value at output, double the iso will require half input light intensity.

        # For cameras with more pixels, assume only the same xmm^2 square of the sensor is used to produce the final image. More pixel density means more samples
        # of the random variables (noise). x times pixel per width gives x^2 times samples of random variables. SNR = 10 log ((S/N)^2) (voltage is measured here not RMS) thus, snr is improved 10 log(sqrt(x^2)^2). (Note here N is the std of the noise, my guess though.)

        # self.min_grey = min(self.min_grey, gray_scale/100/(iso/100.))
        # self.min_snr = min(self.min_snr, np.log2(10) * (snr + np.log(self._pixel_per_width * 1.0 / 6000) / np.log(10) * 20))
        return np.exp(np.power(gray_scale, 1./2.2)), np.log10(10) * (snr + np.log(self._pixel_per_width * 1.0 / 6000 * np.sqrt((iso/100.))) / np.log(10) * 20)
        
        # return gray_scale/100, np.log2(10) * (snr + np.log(self._pixel_per_width * 1.0 / 6000) / np.log(10) * 20)
        # return gray_scale/100/(iso/100.), np.log2(10) * (snr + np.log(self._pixel_per_width * 1.0 / 6000) / np.log(10) * 20)

        # return np.exp(np.power(gray_scale/100/(iso/100.), 1./2.2)), np.log2(10) * (snr + np.log(self._pixel_per_width * 1.0 / 6000) / np.log(10) * 20)

        return np.exp(np.power(gray_scale/100/(iso/100.), 1./2.2)), np.log2(10) * (snr + np.log(self._pixel_per_width * 1.0 / 6000 * np.sqrt(iso/100.)) / np.log(10) * 20)


def add_to_plot(this_camera, iso_name):
    val = this_camera.get_curve(iso_name)
    if val is None:
        return
    # plot gray_scale in log-scale
    a = np.log2(val[:, 0])
    b = val[:, 1] + np.log(1. / val[:, 0]) / np.log(10) * 10 
    legend_name = "{}-{}".format(str(this_camera).ljust(4), iso_name)
    ax = plt.plot(a, val[:, 1], label=legend_name, marker=next(marker_pool))

    
    # legend_name = "{}-{}x".format(str(this_camera).ljust(4), iso_name)
    # ax = plt.plot(a, b, label=legend_name, marker=next(marker_pool))


if __name__ == "__main__":

    series = dict()

    if len(sys.argv) > 1:
        specific_body = sys.argv[1]
        this_camera = CameraBody(specific_body, 6400, 6000)
        for best_iso in all_ISO:
            iso_name = "ISO {}".format(best_iso)
            add_to_plot(this_camera, iso_name)

    else:
        for body_name, metas in body_metadata.items():
            best_iso, pixel_per_width = metas
            iso_name = "ISO {}".format(best_iso)
            this_camera = CameraBody(body_name, best_iso, pixel_per_width)
            add_to_plot(this_camera, iso_name)
        
    # print(this_camera.min_grey, this_camera.min_snr)
    plt.legend()
    plt.show()