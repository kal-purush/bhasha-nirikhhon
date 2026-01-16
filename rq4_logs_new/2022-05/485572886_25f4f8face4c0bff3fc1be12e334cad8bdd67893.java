/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.example.Eventbring.controladores;


import com.example.Eventbring.entidades.Evento;
import com.example.Eventbring.entidades.Usuario;
import com.example.Eventbring.servicios.EventoServicio;
import com.example.Eventbring.servicios.UsuarioServicio;
import java.util.Date;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.ui.ModelMap;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;

/**
 *
 * @author lauta
 */
@Controller
@RequestMapping("/editperfil")
public class EditController {
    
    @Autowired
    private EventoServicio es;
    
    @GetMapping("/modificar/{id}") 
    public String modificar(@PathVariable String id, ModelMap modelo) {

        modelo.put("evento", es.getById(id));

        return "editperfil";
    }
    
    @PostMapping("/modificar/{id}")
    public String modificar(ModelMap modelo, @PathVariable String id, @RequestParam String nombre, @RequestParam Date fecha_hora, @RequestParam Integer cupo, @RequestParam String direccion, @RequestParam String tipo_evento) {

        try {
            Evento e = es.modificar(id, nombre, fecha_hora, cupo, direccion, tipo_evento);
          
            modelo.put("exito", "Modificacion exitosa");

            return "redirect:/miperfil";
        } catch (Exception e) {
            modelo.put("error", "Falto algun dato");
            modelo.put("usuario", es.getById(id));
            return "editperfil";
        }
    }
}