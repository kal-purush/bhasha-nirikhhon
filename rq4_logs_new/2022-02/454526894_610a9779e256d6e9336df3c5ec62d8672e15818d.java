package br.projetoparticularnext.com.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;

import br.projetoparticularnext.com.model.conta.Conta;
import br.projetoparticularnext.com.service.ClienteService;
import br.projetoparticularnext.com.service.ContaService;
import br.projetoparticularnext.com.utils.Const;

@Controller
public class MenuController {
	
	@Autowired
	ContaService contaService;
	
	@Autowired
	ClienteService clienteService;
	
	 
}