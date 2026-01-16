/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package tk.hausplant.controller;

import tk.hausplant.dao.PlantaDAO;
import tk.hausplant.model.Planta;

/**
 * Controller responsável pelas regras de negócio de Planta
 * 
 * @author mateusht
 */
public class PlantaController {
    
    private PlantaDAO plantaDAO;
    
    public Planta carregarPlanta(){
        plantaDAO = new PlantaDAO();
        return plantaDAO.carregar();
    }
    
}