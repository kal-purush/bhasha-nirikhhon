import React, { Fragment } from "react";
import { Route } from "react-router-dom";
import { Routes } from "react-router-dom";
import HomePage from "../Pages/HomePage";
import ListPatient from "../Pages/ListPatient";
import AddPatient from "../Pages/AddPatient";
import ListRv from "../Pages/ListRv";
import AddRv from "../Pages/AddRv";
import AddConsultation from "../Pages/AddConsultation";
import ListConsultation from "../Pages/SecondDashboard/ListConsultation";
import Connection from "../Pages/Connection";
import Setting from "../Pages/Setting";
import Info from "../Pages/Info";
import Fiche from "../Pages/Fiche";
import Informations from "../Pages/Informations";
import NextRv from "../Pages/NextRv";
import DossierMedical from "../Pages/SecondDashboard/DossierMedical";
const MyRoutes = ()=>{
    return(
        <Fragment>

            <Routes>

                <Route  path="/"  element={<HomePage/>} />

                <Route  path="ListPatient" element={<ListPatient/>}  />
                <Route  path="AddPatient" element={<AddPatient/>} />

                <Route path="AddRv" element={<AddRv/>} />
                <Route path="ListRv" element={<ListRv/>} />

                <Route path="Consultation" element={<AddConsultation/>} />

                

                <Route path="/Info/:id" element={<Info />} >
                     
                      <Route  index element={<Fiche/>} />   /Info/:id/DossierMedical/ListConsultation/:idConsultation
                     
                     <Route  path="Fiche" element={<Fiche/>} />

                     <Route  path="Informations" element={<Informations/>} />

                     <Route  path="NextRv" element={<NextRv/>} />

                     <Route  path="DossierMedical" element={<DossierMedical/>} >

                    <Route       path="ListConsultation/:idConsultation" element={<ListConsultation/>} />

                     </Route>
                     
                </Route>
            
                

                <Route       path="Setting" element={<Setting/>} />

                <Route path="Connection" element={<Connection />} />



                
            </Routes>

        </Fragment>

    )
}

export default MyRoutes ;