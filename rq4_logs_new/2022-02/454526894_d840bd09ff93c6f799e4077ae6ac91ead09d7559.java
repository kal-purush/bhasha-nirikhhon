package br.projetoparticularnext.com.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;

import br.projetoparticularnext.com.model.conta.Conta;
import br.projetoparticularnext.com.service.ContaService;
import br.projetoparticularnext.com.utils.Const;

@Controller

public class AreaLogadoController {
	
	@Autowired
	ContaService contaService;
	
	@GetMapping("arealogado/{id}")
	public String buscaConta(@PathVariable(value = "id") long id,Model model) {
		System.err.println("ENTRO NA CONTA: "+id);
		Const.ID_CONTA_LOGADA = id;
		Conta conta = contaService.getConta(Const.ID_CONTA_LOGADA);
		model.addAttribute("conta",conta);
		return "arealogado";
	}
}