import sys
import io
import folium
from PyQt5.QtWidgets import *
from PyQt5.QtWebEngineWidgets import QWebEngineView
from PyQt5.QtCore import *
from PyQt5.QtGui import *
from PyQt5 import uic
import json
import requests
from bs4 import BeautifulSoup
import folium
import pandas as pd
from geopy.distance import distance

#--------------------------------WEB SCRAPING-----------------------------------------------------------
hotels = list()
addresses = list()
stars = list()
urls_direction = list()
direcciones = list()

#CUZCO
url = 'https://www.atrapalo.pe/hoteles/cuzco_d3737'
urlBase = url
count = 2

while count <= 2: #69
    
    page = requests.get(url)
    soup = BeautifulSoup(page.content, 'html.parser')
    
    #Hoteles
    hts = soup.find_all('span', class_="openFontSemiBold")
    for ht in hts:
        hotels.append(ht.get_text(strip = True, separator=' '))

    #Ubicaciones
    ads = soup.find_all('p', class_="item-location")
    for ad in ads:
        addresses.append(ad.get_text(strip = True, separator=' '))

    #Urls de las paginas donde se encuentran las Direcciones
    a_tags = soup.findAll('a', class_ ="hotel-detail-link btn narrow btn-listado", href = True)
    
    for a_tag in a_tags:
        urls_direction.append('http://atrapalo.pe' + a_tag['href'])

    #Estrellas
    infoHotels = soup.find_all('h2')
    for infoHotel in infoHotels:
        countStars = 0 
        stars1 = infoHotel.find_all('span',class_="stars") 
        if len(stars1) >= 1:
            stars2 = stars1[0].find_all('i')
            for star2 in stars2:
                countStars += 1
            stars.append(countStars)
        else:
            stars.append(-1)

    url = urlBase + f'_p{count}'
    count += 1

print(addresses)
#Direcciones
count2 = 0 
url = urls_direction[count2]

while count2 <= len(urls_direction) - 1:
    page = requests.get(url)
    soup = BeautifulSoup(page.content, 'html.parser')
    
    error_load = soup.find('h1', class_ = "atrapaloFont")

    if error_load == None :
        print(urls_direction[count2])
        dir = soup.find('span', class_ = "app-address")
        if dir == None:
            dir = soup.find('span', class_="app-address short-address")
        direcciones.append(dir.get_text(strip = True, separator=' '))

    count2 += 1
    if count2 <= len(urls_direction) - 1:
        url = urls_direction[count2]


#Eliminando Hotel que no carga (Hostal Holiday Hostal)
hotels.remove("Cusco Holiday Hostal")
addresses.remove("Cusco a 2,94 km del centro")
stars.remove(-1)
direcciones.remove("Urb. Kennedy B F-2 Wanchaq, Cusco 84, Cusco (Peru)")
print(len(hotels),len(addresses),len(stars))

#Initializing the column of lat and long
lats = list()
longs = list()
for i in range(len(hotels)):
    lats.append('NaN')
for i in range(len(hotels)):
    longs.append('NaN')

df = pd.DataFrame({'Hotel': hotels, 'Ubicacion': addresses, 
                   'Direccion': direcciones, 'Estrellas': stars, 'Longitud' : longs, 'Latitud': lats}, index=list(range(1,len(hotels)+1)))


#Setting Latitudes and Longitudes with MapQuest API
for i, row in df.iterrows():
    direccion = str(df.at[i,'Direccion'])
    parameters = {
        "key" : "Rkfy036XGdXoOdLnBn5eqnHdiDV2KXYs",
        "location" : direccion
    }
    response = requests.get("http://www.mapquestapi.com/geocoding/v1/address", params = parameters)
    data = json.loads(response.text)['results']

    lng = data[0]['locations'][0]['latLng']['lng']
    lat = data[0]['locations'][0]['latLng']['lat']
    
    df.at[i,'Longitud'] = lng
    df.at[i,'Latitud'] = lat

df.to_csv('Hotels.csv',index=True)
print(df)


#-------------------------------------------------------------FOLIUM-----------------------------------------------------------------------

#Inicializando Folium Map
locationP = -13.524994299145112, -71.95578805057995
m = folium.Map(location = locationP,zoom_start=13, widht = 800, height = 800)

#Inicializando Folium Markers
markers = list()
for i in range(0,len(df)):
   marker = folium.Marker(
      location  =[df.iloc[i]['Latitud'], df.iloc[i]['Longitud']],
      tooltip = (df.iloc[i]['Hotel'],df.iloc[i]['Ubicacion'], "Dir: " + df.iloc[i]['Direccion']
              ,"Estrellas: " + str(df.iloc[i]['Estrellas']))
   ).add_to(m)
   markers.append(marker)

#Inicializando Folium Edges

def searchMarkerInNearestMarkers(marker,nearestMarkersAndDistances):
    boleano = False
    for nearestMarkerAndDistance in nearestMarkersAndDistances:
        if marker == nearestMarkerAndDistance[0]:
            boleano = True
    return boleano

def findNnearests(marker,n):
    nearestMarkersAndDistances = []
    
    while len(nearestMarkersAndDistances) < n:

        Distance = 1000000

        for m in markers:
            if m != marker and searchMarkerInNearestMarkers(m,nearestMarkersAndDistances) == False:
                if distance(marker.location,m.location).km < Distance:
                    nearestMarker = m
                    Distance = distance(marker.location,m.location).km
                    
        nearestMarkersAndDistances.append((nearestMarker,Distance))

    return nearestMarkersAndDistances
    
fedges = list()
for marker in markers:
    nearestMarkersAndDistances = findNnearests(marker,4)
    for nearestMarkerandDistance in nearestMarkersAndDistances:
        fedge = folium.PolyLine((marker.location,nearestMarkerandDistance[0].location),tooltip = nearestMarkerandDistance[1]).add_to(m)
        fedges.append(fedge)



#----------------------------------------------------------GRAPH---------------------------------------------------------------------------

class PriorityQueue:
    pq = []
    def __init__(self,c):
        self.c = c 
    def put(self,e):
        self.pq.append(e)
    def poll(self):
        self.pq.sort(key=self.c)
        e = self.pq.pop(0)
        return e
    def size(self):
        return len(self.pq)
    def print(self):
        print(self.pq)


class Hotel:
    def __init__(self,name,stars,lng,lat,marker):
        self.name = name
        self.stars = stars
        self.lng = lng
        self.lat = lat
        self.marker = marker

    def elements(self):
        return [self.name, self.stars]


hoteles = []
cont = 0
for i in range(len(df)):
        hotel = Hotel(df.iloc[i, 0],df.iloc[i, 3],df.iloc[i, 4],df.iloc[i, 5],markers[cont])
        cont += 1
        hoteles.append(hotel)

def getHotelbyCoordinates(lng,lat):
    for hotel in hoteles:
        if lng == hotel.lng and lat == hotel.lat:
            return hotel



def searchFrAndToFoliumEdge(edgeFolium):
    #Searching the Hotel Fr in the Edge
    for i in range(len(df)):
        if edgeFolium.locations[0][1] == df.iloc[i, 4] and edgeFolium.locations[0][0] == df.iloc[i, 5]:
            hotelFr = getHotelbyCoordinates(df.iloc[i, 4],df.iloc[i, 5])

    #Searching the Hotel To in the Edge
    for i in range(len(df)):
        if edgeFolium.locations[1][1] == df.iloc[i, 4] and edgeFolium.locations[1][0] == df.iloc[i, 5]:
            hotelTo = getHotelbyCoordinates(df.iloc[i, 4],df.iloc[i, 5])

    #print(hotelFr.name,hotelTo.name)
    return (hotelFr,hotelTo)


class Edge:
    def __init__(self, edgeFolium):
        FrAndto = searchFrAndToFoliumEdge(edgeFolium)
        self.fr = FrAndto[0]
        self.to = FrAndto[1]
        self.cost = distance(edgeFolium.locations[0],edgeFolium.locations[1]).km
        self.edgeFolium = edgeFolium


        
class Graph:
    g = {}

    def __init__(self):
        self.loadDataInGraph()
    
    def reverseEdges(self,edges):
        reversedEdges = []
        for edge in edges:
            reversedEdge = Edge(edge.edgeFolium)
            reversedEdge.fr = edge.to
            reversedEdge.to = edge.fr
            reversedEdges.append(reversedEdge)
        return reversedEdges


    def edgeAlreadyExist(self, e):
        for edge in self.g[e.fr]:
            if edge.to == e.to:
                return True
        return False


    def loadDataInGraph(self):
        edges = []
        reversedEdges = []
        
        for fedge in fedges:
            edge = Edge(fedge)
            edges.append(edge)


        reversedEdges = self.reverseEdges(edges)

        for edge in edges:
            if edge.fr not in self.g:
                self.g[edge.fr] = []
            self.g[edge.fr].append(edge)
        
        for reversedEdge in reversedEdges:
            if self.edgeAlreadyExist(reversedEdge) == False:
                self.g[reversedEdge.fr].append(reversedEdge)

            
    
    def printGraph(self):
        for h in self.g:
            print("\n")
            print(h.elements())
            print("Connections")
            for edge in self.g[h]:
                print(edge.to.elements(),edge.cost,"km")
    

    def dfs(self,hotel,visited):
        visited.append(hotel)

        for edge in self.g[hotel]:
            if edge.to not in visited:
                self.dfs(edge.to,visited)
        if hotel not in visited:
            visited.append(hotel)

        return visited


    def cTupple(self,tupple):
        return tupple[1]

    def dijkstra(self, s):
        # el s es un hotel
        vis = {}
        prev = {}
        dist = {}
        pq = PriorityQueue(self.cTupple)
        
        for e in self.g:
            vis[e] = False   
        for e in self.g:
            prev[e] = None
        for e in self.g:
            dist[e] = float("+inf")
        
        dist[s] = 0
        pq.put((s,0))
        
        while pq.size() != 0:
            
            index, minValue = pq.poll() #1
            vis[index] = True #2
            #print(index.name)
            if dist[index] < minValue : continue
            
            for edge in self.g[index]:
                if vis[edge.to] : continue
                newDist = dist[index] + edge.cost
                if newDist < dist[edge.to]:
                    prev[edge.to] = index #3
                    dist[edge.to] = newDist #4
                    pq.put((edge.to, newDist)) #5

        return (dist,prev)
    

    def searchHotelbyName(self,hotelName):
        for hotel in self.g:
            if hotel.name == hotelName:
                return hotel 

    def findShortestPath(self, s, e):
        s = self.searchHotelbyName(s)
        e = self.searchHotelbyName(e)

        dist, prev = self.dijkstra(s)
        path = []
        namesPath = []
        markersPath = []
        hoteles2 = []
        if dist[e] == float("+inf") : return path
        at = e
        while at != None:
            path.insert(0,at)
            at = prev[at]

        for hotel in path:
            namesPath.append(hotel.name)
        
        for hotel in path:
            markersPath.append(hotel.marker)
        
        for hotel in path:
            hoteles2.append(hotel)

        return namesPath, dist[e], markersPath, hoteles2



#--------------------------------------------------CONTROLADORA---------------------------------------------------------------------------
class Controladora:
    g = Graph()

    def __init__(self):
         self.dummy = 5

    def generateMap(self,namesPath,dist,markersPath,hoteles):
        location1 = -13.52260943569571, -71.97032385348659
        m = folium.Map(location = location1, zoom_start = 15, widht = 800, height = 800)

        #Agregando markers
        cont = 0
        for marker in markersPath:
            if cont == len(markersPath) - 1:
                markerFinal = folium.Marker(marker.location, tooltip = (hoteles[cont].name,"Estrellas: " + str(hoteles[cont].stars)),
                 icon=folium.Icon(color='red',icon_color='red')).add_to(m)
            else: 
                markerFinal = folium.Marker(marker.location, tooltip = (hoteles[cont].name,"Estrellas: " + str(hoteles[cont].stars))).add_to(m)
            cont = cont + 1

        #Agregando Edges
        markerIndex = 0
        while markerIndex < len(markersPath) - 1:
            edge = folium.PolyLine((markersPath[markerIndex].location,markersPath[markerIndex+1].location),
                                   tooltip = distance(markersPath[markerIndex].location,markersPath[markerIndex+1].location),color='red',weight = 10).add_to(m)
            markerIndex += 1    

        return m


    def generateMapBySpecificEndHotel(self,startHotel,endHotel):
        namesPath,dist,markersPath,hoteles = self.findShortestPath(startHotel,endHotel)
        m = self.generateMap(namesPath,dist,markersPath,hoteles)
        return m,round(dist,2)

    def generatedMapsByFilters(self,startHotel,starsFilter,maxRangeFilter,nResults):
        hotelNames = []
        distsHotels = []
        maps = list()
        finalMaps = list()
        visited = []
        for hotel in self.dfs(self.searchHotelbyName(startHotel),visited):
            if hotel != self.searchHotelbyName(startHotel) and hotel.stars == int(starsFilter):
                namesPath,dist,markersPath,hoteles = self.findShortestPath(startHotel,hotel.name)
                if dist <= float(maxRangeFilter):
                    m = self.generateMap(namesPath,dist,markersPath,hoteles)
                    maps.append(m)
                    hotelNames.append(hotel.name)
                    distsHotels.append(dist)

        if len(maps) > 0 and nResults <= len(maps):
            cont = 0
            for i in range(nResults):
                finalMaps.append(maps[i])
        else:
            finalMaps = maps

        return finalMaps, hotelNames, distsHotels
        


    def dfs(self,hotel,visited):
        return self.g.dfs(hotel,visited)
    def searchHotelbyName(self,hotelName):
        return self.g.searchHotelbyName(hotelName) 
    def findShortestPath(self, s, e):
        return self.g.findShortestPath(s,e)




#----------------------------------------------------Interfaz Gráfica-----------------------------------------------------------------------

class MainApp(QWidget):
    c = Controladora()

    def __init__(self):
        super().__init__()
        self.window_width, self.window_height = 1600, 900
        self.setMinimumSize(self.window_width,self.window_height)
        self.setWindowTitle("Primera App")
        self.setStyleSheet('background-color: #CBCBCB')
        #Lines
        #self.line1 = QFrame(self)
        #self.line1.setGeometry(30,160,1451,20)
        #Map Container
        self.map_container = QWidget(self)
        self.map_container.setGeometry(510, 560, 631, 281)
        self.map_container_layout = QVBoxLayout(self.map_container)
        self.map_container.setStyleSheet('background-color: gray')

        #Principal Map
        #coordinate = (-21.836337176636032, 134.41795954437035)
        #m = folium.Map(zoom_start = 4,location = coordinate)
        data = io.BytesIO()
        m.save(data, close_file = False)
        self.principalMapWebView = QWebEngineView()
        self.principalMapWebView.setHtml(data.getvalue().decode())
        self.map_container_layout.addWidget(self.principalMapWebView)
        
        #--------Inputs-------
        #Hotel Inicio
        self.startHotelLabel = QLabel(self)
        self.startHotelLabel.setText("¿En qué hotel te encuentras?")
        self.startHotelLabel.setGeometry(720,80,171,16)
        self.startHotel = QComboBox(self)
        self.startHotel.setGeometry(630,110,341,21)  
        for i in range(len(hotels)):
            self.startHotel.addItem(hotels[i])
        #Hotel Destino Especifico 
        self.endHotelLabel = QLabel(self)
        self.endHotelLabel.setText("Busca un hotel en específico")
        self.endHotelLabel.setGeometry(330,180,171,21)
        self.endHotel = QComboBox(self)
        self.endHotel.setGeometry(230,280,341,21)   
        for i in range(len(hotels)):
            self.endHotel.addItem(hotels[i])
        #Filtros
        self.labelFilters = QLabel(self)
        self.labelFilters.setText("Filtros")
        self.labelFilters.setGeometry(1200,180,41,16)
        #Stars Filter
        self.starsLabel = QLabel(self)
        self.starsLabel.setText("Estrellas:")
        self.starsLabel.setGeometry(920,290,55,16)
        self.radioButtons = list()
        self.radioButton_1 = QRadioButton("1",self)
        self.radioButton_1.setGeometry(1020,230,95,20)
        self.radioButton_1.setChecked(True)
        self.radioButton_2 = QRadioButton("2",self)
        self.radioButton_2.setGeometry(1020,260,95,20)
        self.radioButton_3 = QRadioButton("3",self)
        self.radioButton_3.setGeometry(1020,290,95,20)
        self.radioButton_4 = QRadioButton("4",self)
        self.radioButton_4.setGeometry(1020,320,95,20)
        self.radioButton_5 = QRadioButton("5",self)
        self.radioButton_5.setGeometry(1020,350,95,20)
        self.radioButtons.append(self.radioButton_1)
        self.radioButtons.append(self.radioButton_2)
        self.radioButtons.append(self.radioButton_3)
        self.radioButtons.append(self.radioButton_4)
        self.radioButtons.append(self.radioButton_5)
        #Max Range Filter
        self.maxRangeLabeld = QLabel(self)
        self.maxRangeLabeld.setText("Distancia:")
        self.maxRangeLabeld.setGeometry(1190,370,55,16)
        self.maxRangeLabel = QLabel(self)
        self.maxRangeLabel.setGeometry(1170,250,31,20)
        self.maxRangeFilter2 = QSlider(self)
        self.maxRangeFilter2.setOrientation(Qt.Vertical)
        self.maxRangeFilter2.setGeometry(1210,250,21,101)
        self.maxRangeFilter2.setTickPosition(QSlider.TicksRight)
        self.maxRangeFilter2.setTickInterval(5)
        self.maxRangeFilter2.setMinimum(0)
        self.maxRangeFilter2.setMaximum(15)
        self.maxRangeFilter2.setToolTip(self.maxRangeLabel.text())
        self.maxRangeFilter2.valueChanged.connect(self.changed_slider)
        #N Results Filter
        self.nResultsLabel = QLabel(self)
        self.nResultsLabel.setText("N resultados")
        self.nResultsLabel.setGeometry(1400,290,71,16)
        self.nResultsFilter = QSpinBox(self)
        self.nResultsFilter.setGeometry(1420,320,42,22)
        #Validation
        self.noResultsLabel = QLabel(self)
        self.noResultsLabel.setText("No se encontraron resultados")
        self.noResultsLabel.setGeometry(520,530,255,16)
        self.noResultsLabel.setStyleSheet('color : red;')
        self.noResultsLabel.hide()
        self.labelResultSpecificHotel = QLabel(self)
        self.labelResultSpecificHotel.setGeometry(520,530,700,30)
        self.labelResultSpecificHotel.hide()
        self.hotelNames = []
        self.distsHotels = []


        #------Buttons------

        #Button Search (Specific Hotel)
        self.mapWebViewSpecificHotel = QWebEngineView()
        self.map_container_layout.addWidget(self.mapWebViewSpecificHotel)
        self.mapWebViewSpecificHotel.hide()
        self.buttonSearch = QPushButton("Search",self)
        self.buttonSearch.setGeometry(370,440,93,28)
        self.buttonSearch.clicked.connect(lambda :self.search("search"))
        
        #Button Filter
        self.mapWebViews = list()
        self.buttonFilter = QPushButton("Filter",self)
        self.buttonFilter.setGeometry(1180,440,93,28)
        self.buttonFilter.clicked.connect(lambda :self.search("filter"))

        self.indexCurrentWebView = 0
        #Button Back    
        self.buttonBack = QPushButton("Back",self)
        self.buttonBack.setGeometry(410, 680, 81, 28)
        self.buttonBack.clicked.connect(lambda :self.BackNextMap("back"))
        self.buttonBack.hide()
        #Button Next
        self.buttonNext = QPushButton("Next",self)
        self.buttonNext.setGeometry(1150, 680, 81, 28)
        self.buttonNext.clicked.connect(lambda :self.BackNextMap("next"))
        self.buttonNext.hide()

    def changed_slider(self):
        value = self.maxRangeFilter2.value()
        self.maxRangeLabel.setText(str(value))

    def BackNextMap(self, action):
        if len(self.mapWebViews) > 0:
            if action == "back":
                if self.indexCurrentWebView > 0:
                    print(self.indexCurrentWebView)
                    self.mapWebViews[self.indexCurrentWebView].hide()
                    self.indexCurrentWebView -= 1
                    print(self.indexCurrentWebView)
                    self.mapWebViews[self.indexCurrentWebView].show()
            elif action == "next":
                if self.indexCurrentWebView < len(self.mapWebViews) - 1:
                    print(self.indexCurrentWebView)
                    self.mapWebViews[self.indexCurrentWebView].hide()
                    self.indexCurrentWebView += 1
                    print(self.indexCurrentWebView)
                    self.mapWebViews[self.indexCurrentWebView].show()

        self.labelResultSpecificHotel.setText("Resultado " + str(self.indexCurrentWebView+1) + " de "+str(len(self.mapWebViews)) +": "+ self.hotelNames[self.indexCurrentWebView]
                +"\nLa ruta más corta es de " + str(round(self.distsHotels[self.indexCurrentWebView],2)))


    def search(self, type):

        if type == "search": #Si se presiona Search Button
            if len(self.mapWebViews) > 0:
                for mapWebView in self.mapWebViews:
                    mapWebView.hide()
            self.principalMapWebView.hide()
            self.buttonBack.hide()
            self.buttonNext.hide()
            self.noResultsLabel.hide()

            m,dist = self.generateMapBySpecificEndHotel()
            data = io.BytesIO()
            m.save(data, close_file = False)
            self.mapWebViewSpecificHotel.setHtml(data.getvalue().decode())
            self.mapWebViewSpecificHotel.show()
            self.labelResultSpecificHotel.setText("Resultado:\nLa ruta más corta a "+self.endHotel.currentText()+" es de "+str(dist) +" km aprox.")
            self.labelResultSpecificHotel.show()


        elif type == "filter":# Si se presiona Filter Button

            #----Maps Generados por los Filtros----#
            
            maps,self.hotelNames,self.distsHotels = self.generateMapsByFilters()
            
            if len(maps) > 0:
                self.labelResultSpecificHotel.show()
                
                self.noResultsLabel.hide()
                self.mapWebViewSpecificHotel.hide()
                self.buttonBack.show()
                self.buttonNext.show()
                self.indexCurrentWebView = 0
                if len(self.mapWebViews) > 0:
                    for mapWebView in self.mapWebViews:
                        mapWebView.hide()
                self.mapWebViews.clear()
                
                
                #----MapWebViews----#
                #Cargar los mapas en los webViews
                
                for m in maps:
                    data = io.BytesIO()
                    m.save(data, close_file = False)
                    mapWebView = QWebEngineView()
                    mapWebView.setHtml(data.getvalue().decode())
                    self.mapWebViews.append(mapWebView)

                self.labelResultSpecificHotel.setText("Resultado 1 de "+str(len(self.mapWebViews)) +": "+ self.hotelNames[0]
                +"\nLa ruta más corta es de " + str(round(self.distsHotels[0],2)))

                #Primero Añadimos los webViews al layout del map_container y los ocultamos
                for mapWebView in self.mapWebViews:
                    self.map_container_layout.addWidget(mapWebView)
                    mapWebView.hide()

                #Segundo ocultamos el mapWebView Inicial o Principal (mapa con todos los nodos hoteles y aristas)
                self.principalMapWebView.hide()

                #Mostramos en pantalla el primer mapWeview de los resultados
                self.mapWebViews[0].show()
            else:
                self.labelResultSpecificHotel.hide()
                self.noResultsLabel.show()
                self.buttonBack.hide()
                self.buttonNext.hide()



    def generateMapsByFilters(self):
        for radioButton in self.radioButtons:
            if radioButton.isChecked():
                stars = radioButton.text()
                break

        maps = list()
        visited = []
        maps,hotelNames,distsHotels = self.c.generatedMapsByFilters(self.startHotel.currentText(),stars,str(self.maxRangeFilter2.value()),int(self.nResultsFilter.value())) 
        return maps,hotelNames,distsHotels   

    def generateMapBySpecificEndHotel(self):
        return self.c.generateMapBySpecificEndHotel(self.startHotel.currentText(),self.endHotel.currentText())



    def generateMap(self,namesPath,dist,markersPath,hoteles):
        return self.c.generateMap(namesPath,dist,markersPath,hoteles)
        
            
if __name__ == '__main__':
    app = QApplication([])
    window = MainApp()
    window.show()
    app.exec_()