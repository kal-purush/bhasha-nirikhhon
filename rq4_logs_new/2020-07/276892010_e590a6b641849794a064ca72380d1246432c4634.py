#author Nikita



import numpy as np
import matplotlib.pyplot as plt

import Helper

class Vizual2:
    def open(self, json_container):
        data = self.GetData(json_container)
        print(data)

        fig, (self.ax1, self.ax2) = plt.subplots(2, sharey=True, figsize=(8, 6))
        self.ax1.plot(data[2][0], data[1][0], 'ko-')
        self.ax1.set(title=data[0][0], ylabel='Кол-во камер')

        self.ax2.plot(data[2][1], data[1][1], 'ko-')
        self.ax2.set(xlabel=data[0][1], ylabel='Кол-во камер')

        plt.show()

    def GetData(self, container):
        cameraInfo = Helper.JsonList(container)

        admArea = cameraInfo.GetAllUniquWalue("AdmArea")
        count_admArea = cameraInfo.GetEqualRowsCount("Центральный административный округ", admArea)

        sortedCamera = sorted(count_admArea.GetList(), key=lambda camera: camera[1])

        least_cameras = {sortedCamera[0][0]: {}, sortedCamera[1][0]: {}}

        n_data = {}

        for area in [i for i, j in least_cameras.items()]:
            for camera in container:
                if camera["AdmArea"] == area:
                    if least_cameras[area].get(camera["District"]) != None:
                        least_cameras[area][camera["District"]] += 1
                    else:
                        least_cameras[area][camera["District"]] = 1

        end_data = [[i for i, j in least_cameras.items()],
                    [[item for k, item in j.items()] for i, j in least_cameras.items()],
                    [[self.MakeAcronym(k) for k, item in j.items()] for i, j in least_cameras.items()]]

        return end_data

    def MakeAcronym(self, string):
        n_str = ""
        for item in string.split(" "):
            n_str += item[:3] + " "

        return n_str

    def close(self, json_container):
        print("")