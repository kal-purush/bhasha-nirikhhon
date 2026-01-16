import Helper
import numpy as np
import matplotlib.pyplot as plt
import os
import folium
import webbrowser
import shutil

class FullMupVisualizationProjectEvgen:

    def color_picker(self, value, max, min):
        scale = (max - min) / 510
        color = value - min
        if value == min:
            return "#00ff00"
        elif value == max:
            return "#ff0000"

        color /= scale
        color = int(color)
        
        if color < 256:
            color = "#"+hex(color)[2:]+"ff00"        
        else:
            color-=255
            color = 255-color
            color = "#ff"+hex(color)[2:]+"00"
        return color

    def open(self, json_container):
        map = folium.Map(location=[55.753228, 37.622480], zoom_start = 9)

        geodata = self.makeGeoData(json_container)
        folium.GeoJson(
            "http://gis-lab.info/data/mos-adm/ao.geojson",
            name='geojson',
        ).add_to(map)

        for area in geodata[2]:
            folium.CircleMarker(location=[area["lan"], area["lat"]], radius = 9, popup = str(area["AdmArea"]), fill_color=self.color_picker(area["count"], geodata[0], geodata[1]), color="gray", fill_opacity = 0.9).add_to(map)

        folium.LayerControl().add_to(map)

        os.mkdir(".\\temp")

        map.save(".\\temp\\map1.html")
        webbrowser.open('.\\temp\\map1.html', new=1)

        sortedCameraCounter = Helper.JsonList(sorted(geodata[2], key=lambda camera: camera["count"]))

        data = sortedCameraCounter.MakeComparisonData("AdmArea", "count").GetList()

        print(data)

        X = np.arange(len(data[0]))

        self.fig, self.ax = plt.subplots(figsize=(8,6))
        self.fig.canvas.set_window_title('Проверка суждения о количестве камер в районах ЦАО')

        rects = self.ax.bar(X + 0.00, data[1], color = 'b', width = 0.50, label="камеры")

        self.ax.set_ylabel('Scores')
        self.ax.set_title("Количество камер по округам")
        self.ax.set_xticks(X)
        self.ax.set_xticklabels(data[0])
        self.ax.legend()

        self.autolabel(rects, "left")

        self.fig.tight_layout()

        plt.show()

    def SwupCoordinates(self, coordinates):
        return [coordinates[1],coordinates[0]]

    def makeGeoData(self, collection):
        ptr_collection = {}
        n_collection = list()

        maxCount=-1
        minCount=-1

        for camera in collection:
            val = ptr_collection.get(camera["AdmArea"])
            if val == None:
                ptr_collection[camera["AdmArea"]] = len(ptr_collection)
                n_collection.append({
                    "AdmArea": camera["AdmArea"],
                    "geoData": [camera["geoData"]["coordinates"]]
                })
            else:
                n_collection[val]["geoData"].append(camera["geoData"]["coordinates"])
        
        for camera in n_collection:
            latcof = 0.0
            lancof = 0.0
            count = 0
            for point in camera["geoData"]:
                latcof+=point[0]-camera["geoData"][0][0]
                lancof+=point[1]-camera["geoData"][0][1]
                count+=1
            if maxCount < count:
                maxCount = count
            if minCount > count or minCount == -1:
                minCount = count
            camera["count"] = count
            camera["lat"] = camera["geoData"][0][0]+(latcof/count)
            camera["lan"] = camera["geoData"][0][1]+(lancof/count)

        return [maxCount, minCount, n_collection]

    def autolabel(self, rects, xpos='center'):
        ha = {'center': 'center', 'right': 'left', 'left': 'right'}
        offset = {'center': 0, 'right': 1, 'left': -1}

        for rect in rects:
            height = rect.get_height()
            self.ax.annotate('{}'.format(height),
                        xy=(rect.get_x() + rect.get_width() / 2, height),
                        xytext=(offset[xpos]*3, 3),  # use 3 points offset
                        textcoords="offset points",  # in both directions
                        ha=ha[xpos], va='bottom')

    def deleteTempFolder(self):
        if os.path.exists("temp"):
            path = os.path.join(os.path.abspath(os.path.dirname(__file__)), 'temp')
            shutil.rmtree(path)


    def close(self, json_container):
        plt.close(self.fig)
        self.deleteTempFolder()