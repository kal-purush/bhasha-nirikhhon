/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.example.Eventbring.controladores;

import com.example.Eventbring.servicios.EventoServicio;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;

/**
 *
 * @author lauta
 */
@Controller
@RequestMapping("/listaeventos")
public class ListController {
    
    @Autowired
    private EventoServicio es;
    
    @GetMapping("/asistir/{id}")
    public String asistir(@PathVariable String id) {

        try {
            
            es.reducirCupo(id);
            
            return "redirect:/listaeventos";
        } catch (Exception e) {
            return "redirect:/listaeventos";
        }

    }
    
    @GetMapping("/borrar/{id}")
    public String borrar(@PathVariable String id) {

        try {
            
            es.borrar(id);
            
            return "redirect:/listaeventos";
        } catch (Exception e) {
            return "redirect:/listaeventos";
        }

    }
    
}