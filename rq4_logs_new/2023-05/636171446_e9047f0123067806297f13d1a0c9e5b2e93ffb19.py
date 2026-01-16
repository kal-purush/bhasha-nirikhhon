import streamlit as st
from streamlit_folium import st_folium
from zipfile import ZipFile
from geotexxx.gefxml_reader import Cpt, Bore
import pandas as pd
import numpy as np
from pyproj import Transformer
import folium

st.markdown(
    """
### BRO plotter

Deze applicatie maakt grafieken van bestanden die via de BRO gedownload kunnen worden.

Werkwijze;
* Ga naar https://www.broloket.nl/ondergrondgegevens
* Maak een aanvraag voor gegevens met de rechthoek of de polygoon tool
* Klik op 'gegevens aanvragen' en vul de vereiste gegevens in
* Je krijgt een mail met daarin een link naar een zip bestand
* Upload dit zip bestand naar deze app en er worden grafieken gemaakt van je gegevens

#### Ontwikkelaar

Rob van Putten | Breinbaas | LeveeLogic | Pecto

#### Opmerkingen

Let op dat er in de gebruikte package voor het lezen van sonderingen en boringen (geotexxx ontwikkeld door Thomas van der Linden)
nog fouten optreden bij sommige sondeer- en boorbestanden. Deze fouten worden bovenin de applicatie uitvoer weergegeven. 
Lange leve het XML bestand ;-)?

#### TODO

* XML fouten proberen op te lossen in PB naar geotexxx
* Zoomen op de map ververst ook de plots en dat is langzaam en onnodig
* Zou leuk zijn om de markers op de map interactief te maken (met popup van grafiek of met verspringen naar grafiek in het document)

#### Choose uploadfile
"""
)
uploaded_file = st.file_uploader("Choose a BRO zip file")


# st.pyplot(fig)

cpt_figs = []
borehole_figs = []
cpt_errors = []
borehole_errors = []

tf = Transformer.from_crs(28992, 4326)

if uploaded_file is not None:
    mapinfo = []
    with ZipFile(uploaded_file) as zf:
        for file in zf.namelist():
            if "sondeer" in file and file.endswith(".xml"):
                try:
                    with zf.open(file) as f:
                        cpt = Cpt()
                        cpt.load_xml(f.read(), fromFile=False)
                        fig = cpt.plot(saveFig=False)
                        cpt_figs.append((file, fig))
                        lat, lon = tf.transform(cpt.easting, cpt.northing)
                        mapinfo.append(("cpt", file, lat, lon))
                except Exception as e:
                    print("1")
                    cpt_errors.append(f"Could not read '{file}' got error '{e}'")
            elif "boor" in file and file.endswith(".xml"):
                try:
                    with zf.open(file) as f:
                        borehole = Bore()
                        borehole.load_xml(f.read(), fromFile=False)
                        fig = borehole.plot(saveFig=False)
                        borehole_figs.append((file, fig))
                        lat, lon = tf.transform(borehole.easting, borehole.northing)
                        mapinfo.append(("borehole", file, lat, lon))
                except Exception as e:
                    borehole_errors.append(f"Could not read '{file}' got error '{e}'")

    if len(cpt_errors) > 0:
        st.markdown("#### Leesfouten bij de sonderingen")
        for e in cpt_errors:
            st.markdown(e)

    if len(borehole_errors) > 0:
        st.markdown("#### Leesfouten bij de boringen")
        for e in borehole_errors:
            st.markdown(e)

    df = pd.DataFrame({"lat": [e[2] for e in mapinfo], "lon": [e[3] for e in mapinfo]})
    midpoint = (np.average(df["lat"]), np.average(df["lon"]))
    m = folium.Map(location=midpoint, zoom_start=16)

    for t, f, lat, lon in mapinfo:
        print(t, f, lat, lon)
        if t == "cpt":
            folium.CircleMarker(
                [lat, lon],
                radius=5,
                tooltip=f,
                color="blue",
                fillcolor="blue",
            ).add_to(m)
        else:
            folium.CircleMarker(
                [lat, lon], radius=5, tooltip=f, color="red", fillcolor="red"
            ).add_to(m)

    st.markdown("#### Locaties")
    st_folium(m, width=800)

    if len(cpt_figs) > 0:
        st.markdown("#### Sonderingen")
        for filename, fig in cpt_figs:
            st.markdown(f"#### {filename}")
            st.pyplot(fig)

    if len(borehole_figs) > 0:
        st.markdown("#### Boringen")
        for filename, fig in borehole_figs:
            st.markdown(f"#### {filename}")
            st.pyplot(fig)