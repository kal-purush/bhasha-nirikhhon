/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package Iservices;

import Entite.Users;

/**
 *
 * @author khaled22
 */
public interface InterfaceUsers {
    
     public void verifierLogin(Users u);
     public void addPatient( Users u);
     public void addMedecin( Users u);
     public void addPharmacien( Users u);
     public void addPropCentre( Users u);
    
}