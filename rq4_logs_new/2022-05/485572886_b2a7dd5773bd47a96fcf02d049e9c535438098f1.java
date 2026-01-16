/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.example.Eventbring.controladores;


import com.example.Eventbring.entidades.Evento;
import com.example.Eventbring.servicios.EventoServicio;

import java.util.Date;

import javax.servlet.http.HttpSession;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;

/**
 *
 * @author lauta
 */
@Controller
@RequestMapping("/cargareventos")
public class EventoController {
    @Autowired
    private EventoServicio es;
    
    @GetMapping("/registro")
    public String cargareventos() {
        return "cargareventos";
    }
    
    @PostMapping("/registro")
    public String guardarEvento (@RequestParam String nombre, @RequestParam Date fecha_hora, @RequestParam Integer cupo, @RequestParam String direccion, @RequestParam String tipo_evento){
       try {

          es.registrar(nombre, fecha_hora, cupo, direccion, tipo_evento);
           
       } catch (Exception e) {
           System.out.println(e.getMessage());
       }
       return "redirect:/cargareventos"; 
    }
    
}