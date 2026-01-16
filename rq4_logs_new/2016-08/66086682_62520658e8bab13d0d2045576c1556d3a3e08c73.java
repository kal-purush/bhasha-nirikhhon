package br.com.PersistSpring.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.servlet.ModelAndView;

import br.com.PersistSpring.modelo.Fornecedor;
import br.com.PersistSpring.modelo.Fornecedor;
import br.com.PersistSpring.servico.FornecedorServico;


@Controller
public class FornecedorController {
	
	@Autowired
	FornecedorServico fornecedorServico;
	

	@RequestMapping("/cadastraFornecedor")
	public ModelAndView cadastraFornecedor(Model model,Fornecedor fornecedor) {
			model.addAttribute("mensagem",	fornecedorServico.salvar(fornecedor));
		return new ModelAndView("/fornecedor");
	}
	
	
	@RequestMapping("/pesquisarFornecedor")
	public ModelAndView pesquisarFornecedor(Model model) {
			model.addAttribute("listaFornecedor",	fornecedorServico.pesquisarFornecedor());
		return new ModelAndView("/fornecedor");
	}
	
	@RequestMapping("/pesquisarFornecedorNome")
	public ModelAndView pesquisarFornecedor(Model model,Fornecedor fornecedor) {
			model.addAttribute("listaFornecedorNome",	fornecedorServico.pesquisarFornecedorNome(fornecedor));
		return new ModelAndView("/fornecedor");
	}
	



	


}