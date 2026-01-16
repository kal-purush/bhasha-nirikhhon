package com.fra.clientes.controller.mvc;



import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.servlet.ModelAndView;

import com.fra.clientes.models.Cliente;
import com.fra.clientes.services.ClienteService;

@Controller
public class ClientesController {

	@Autowired
	private ClienteService clienteService;

	@Autowired
	private com.fra.clientes.controller.rest.ClientesController clientesController;
	
	@RequestMapping(value = "/home" , method = RequestMethod.GET)
	public ModelAndView home() {
		
		//TODO: MANEJO LA RESPUESTA DE JSON
		List<Cliente> clientes = clientesController.cliente();
		
		ModelAndView modelAndView = new ModelAndView("Home");
		modelAndView.addObject("Lista", clientes);
		
		return modelAndView;
	}

	@RequestMapping(value = "/cliente/form/add" , method = RequestMethod.GET)
	public ModelAndView formAddCliente() {
		ModelAndView modelAndView = new ModelAndView("NewCliente");
		return modelAndView;
	}
	
	@RequestMapping(value = "/cliente/{id}/form/update" , method = RequestMethod.GET)
	public ModelAndView formUpdateCliente(@PathVariable("id") long id) {
		
		Cliente cliente = clienteService.getClienteById(id);
		ModelAndView modelAndView = new ModelAndView("EditCliente");
		modelAndView.addObject("Cliente", cliente);
		
		return modelAndView;
	}	

}