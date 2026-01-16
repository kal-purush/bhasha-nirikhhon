import bge

#cont-nimiseen muuttujaan säilötään nykyinen controlleri, eli H:hon kytketty
#Python-ohjain, johon valittiin Tallennus.py
cont = bge.logic.getCurrentController()

#sce-nimiseen muuttujaan säilötään koko pelin sisältävä maailma
sce = bge.logic.getCurrentScene()

#own-nimiseen muuttujaan säilötään cont-muuttujan omistaja, eli peliobjekti, jonka
#sisällä se on, tässä tapauksessa MenuKeskus
own = cont.owner

sce.addObject("Cube","MenuKeskus")
own.worldPosition = [0.12419,-0.21373,-2.17934]

sce.addObject("Plane","MenuKeskus")
own.worldPosition = [4.07625,1.00545,5.90386]

sce.addObject("Lamp","MenuKeskus")