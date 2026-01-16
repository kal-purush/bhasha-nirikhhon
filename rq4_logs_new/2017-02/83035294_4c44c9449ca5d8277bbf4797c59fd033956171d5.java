/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package Controlador;

import javax.servlet.http.HttpServletRequest;
import org.springframework.stereotype.Controller;
import org.springframework.ui.ModelMap;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.servlet.ModelAndView;

/**
 *
 * @author jonathan
 */
@Controller 
public class Controlador {
    
    @RequestMapping(value="/")
    public String inicio(){
        return "inicio";
    }
    
    @RequestMapping(value="/persona1", method = RequestMethod.GET)
    public ModelAndView persona1(ModelMap model,HttpServletRequest request){
        String p = request.getParameter("nombre1");
        model.addAttribute("persona", p);
        return new ModelAndView("persona",model);
    
    }
    
    @RequestMapping(value="/persona2", method = RequestMethod.POST)
    public ModelAndView persona2(ModelMap model,HttpServletRequest request){
        String p = request.getParameter("nombre2");
        model.addAttribute("persona", p);
        return new ModelAndView("persona",model);
    
    }
}