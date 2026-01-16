package br.projetoparticularnext.com.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.servlet.ModelAndView;

import br.projetoparticularnext.com.model.conta.Conta;
import br.projetoparticularnext.com.service.ContaService;
import br.projetoparticularnext.com.service.PixService;
import br.projetoparticularnext.com.service.cartao.SeguroService;
import br.projetoparticularnext.com.utils.Const;

@Repository
@RequestMapping("/menu/")
public class MenusController {
	
	@Autowired
	ContaService contaService;
	
	@Autowired
	PixService pixService;
	
	@Autowired
	SeguroService seguroService;
	
	@GetMapping("pix")
	public ModelAndView chamaMenuPix(ModelAndView modelAndView) {
		Conta conta = contaService.getConta(Const.ID_CONTA_LOGADA);
		pixService.verificaPix(modelAndView, conta,"");
		modelAndView.setViewName("redirect:/arealogado/pix/");
		System.err.println("CHAMOU MENU PIX");
		return modelAndView;
	}
	@GetMapping("cartoes")
	public ModelAndView chamaMenuCartoes(ModelAndView modelAndView) {
		Conta conta = contaService.getConta(Const.ID_CONTA_LOGADA);
		System.err.println("CHAMOU MENU CARTOES");
		return modelAndView;
	}
	@GetMapping("arealogado")
	public String chamaMenuLogado(Model modelAndView) {
		Conta conta = contaService.getConta(Const.ID_CONTA_LOGADA);
		modelAndView.addAttribute("conta",conta);
		System.err.println("CHAMOU MENU AREA LOGADO");
		return "redirect:/arealogado/"+conta.getId();
	}
	
	@GetMapping("seguros")
	public ModelAndView chamaMenuSeguros(ModelAndView modelAndView) {
		modelAndView.setViewName("redirect:/arealogado/cartao/credito/seguro");
		Conta conta = contaService.getConta(Const.ID_CONTA_LOGADA);
//		seguroService.verificaSeguro(modelAndView, conta,"");
		System.err.println("CHAMOU MENU SEGUROS");
		return modelAndView;
	}

	
	
}