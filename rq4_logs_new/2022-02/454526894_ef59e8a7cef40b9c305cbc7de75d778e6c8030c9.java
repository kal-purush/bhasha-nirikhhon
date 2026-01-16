package br.projetoparticularnext.com.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.validation.BindingResult;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.servlet.ModelAndView;
import org.springframework.web.servlet.mvc.support.RedirectAttributes;

import br.projetoparticularnext.com.model.cartao.CartaoCredito;
import br.projetoparticularnext.com.model.cartao.Compra;
import br.projetoparticularnext.com.model.conta.Conta;
import br.projetoparticularnext.com.service.ContaService;
import br.projetoparticularnext.com.service.cartao.CartaoCreditoService;
import br.projetoparticularnext.com.utils.Const;
import br.projetoparticularnext.com.utils.Utils;

@Controller
@RequestMapping("/arealogado/cartao/credito/")
public class CartaoCreditoController {

	@Autowired
	CartaoCreditoService cartaoCreditoService;

	@Autowired
	ContaService contaService;
	
	@GetMapping
	public ModelAndView index(ModelAndView modelAndView) {
		modelAndView.setViewName("cartao/credito/cartaoCredito");
		Conta conta = contaService.getConta(Const.ID_CONTA_LOGADA);
		cartaoCreditoService.verificaCartao(modelAndView, conta);
		return modelAndView;
	}
	
	@PostMapping("criar")
	public ModelAndView create( ModelAndView modelAndView,
			RedirectAttributes attributes, CartaoCredito credito, BindingResult result) {
		System.err.println("ANTES DO IF");
		if (!result.hasErrors()) {
			Conta conta = contaService.getConta(Const.ID_CONTA_LOGADA);
			System.err.println("ENTROU NO CADASTRA");
			credito.setAtivo(true);
			credito.setNumero(Utils.geraBlocosNumeros(4));
			credito.setLimite(conta.getCliente().getTipo().getLimite());
			conta.setCredito(credito);
			contaService.createConta(conta);
			modelAndView.setViewName("cartao/credito/cartaoCredito");
			modelAndView.addObject("ok", "Cartao de credito criado com limite aprovado de "+credito.getLimite());
			cartaoCreditoService.verificaCartao(modelAndView, conta);
			return modelAndView;
		} else {
			modelAndView.addObject("erro", "Houve um erro no cartao!");
			modelAndView.setViewName("cartao/credito/cartaoCredito");
			return modelAndView;
		}
	}
	@GetMapping("criar")
    public ModelAndView chamaCriar( ModelAndView modelAndView,CartaoCredito credito) {
		System.err.println("entrou criar");
    	Conta conta = contaService.getConta(Const.ID_CONTA_LOGADA);
    	modelAndView.addObject("conta",conta);
    	modelAndView.setViewName("cartao/credito/criarCredito");
		return modelAndView;
    }
	@GetMapping("ativar")
    public ModelAndView chamaAtivacao( ModelAndView modelAndView,CartaoCredito credito) {
		System.err.println("entrou Ativar");
    	Conta conta = contaService.getConta(Const.ID_CONTA_LOGADA);
    	modelAndView.addObject("conta",conta);
    	conta.getCredito().setAtivo(!conta.getCredito().isAtivo());
    	cartaoCreditoService.verificaCartao(modelAndView, conta);
    	contaService.createConta(conta);
    	modelAndView.setViewName("cartao/credito/cartaoCredito");
		return modelAndView;
    }
	@GetMapping("deletar")
    public ModelAndView chamaDelete( ModelAndView modelAndView,CartaoCredito credito) {
		System.err.println("entrou deletar");
    	Conta conta = contaService.getConta(Const.ID_CONTA_LOGADA);
    	modelAndView.addObject("credito",conta.getCredito());
    	modelAndView.setViewName("cartao/credito/deletarCredito");
		return modelAndView;
    }
	@PostMapping("deletar")
	public ModelAndView delete( ModelAndView modelAndView,
			RedirectAttributes attributes, CartaoCredito credito, BindingResult result) {
		System.err.println("ANTES DO IF");
		if (!result.hasErrors()) {
			Conta conta = contaService.getConta(Const.ID_CONTA_LOGADA);
			System.err.println("ENTROU NO DELETAR CREDITO");
			if(conta.getCredito().getValorFatura() == 0 && conta.getCredito().getApolice() == null) {
				cartaoCreditoService.delete(conta.getCredito().getId());
				conta.setCredito(null);
				contaService.createConta(conta);
				modelAndView.setViewName("cartao/credito/cartaoCredito");
				modelAndView.addObject("ok", "Cartao de crédito deletado com sucesso!");
				cartaoCreditoService.verificaCartao(modelAndView, conta);
				return modelAndView;
			}else {
				modelAndView.setViewName("cartao/credito/cartaoCredito");
				modelAndView.addObject("erro", "Não pode excluir cartão pois possui pendencias!");
				cartaoCreditoService.verificaCartao(modelAndView, conta);
				return modelAndView;
			}
		} else {
			modelAndView.addObject("erro", "Houve um erro ao deletar cartao!");
			modelAndView.setViewName("cartao/credito/cartaoCredito");
			return modelAndView;
		}
	}
	
	@GetMapping("comprar")
    public ModelAndView chamaComprar( ModelAndView modelAndView) {
		System.err.println("entrou comprar");
    	Conta conta = contaService.getConta(Const.ID_CONTA_LOGADA);
    	modelAndView.addObject("conta",conta);
    	modelAndView.setViewName("cartao/credito/comprarCredito");
		return modelAndView;
    }
	
	@PostMapping("comprar")
	public ModelAndView comprar( ModelAndView modelAndView,
			RedirectAttributes attributes, Compra compra,String senha, BindingResult result) {
		System.err.println("COMPRAR O QUE VEIO DO FOMR: "+senha+" "+compra.getDescricao()+" "+compra.getValor());
		if (!result.hasErrors()) {
			Conta conta = contaService.getConta(Const.ID_CONTA_LOGADA);
			System.err.println("ENTROU NO COMPRA");
			if(conta.getCredito().getSenha().equals(senha)) {
				if(conta.getCredito().getLimite() >= compra.getValor()) {
					if(conta.getCredito().getLimite() >= compra.getValor()) {
						conta.getCredito().setLimite(conta.getCredito().getLimite()-compra.getValor());
						conta.getCredito().setValorFatura(conta.getCredito().getValorFatura() + compra.getValor());
						compra.setDataCompra(Utils.dataAtual());
						compra.setCartao(conta.getCredito());
						conta.getCredito().getCompras().add(compra);
						contaService.createConta(conta);
						modelAndView.setViewName("cartao/credito/comprarCredito");
						modelAndView.addObject("ok", "Compra efetuada com sucesso!");
						return modelAndView;
					}else {
						modelAndView.setViewName("cartao/credito/comprarCredito");
						modelAndView.addObject("erro", "Voce não possui Limite!");
						return modelAndView;
					}
				}else {
					modelAndView.setViewName("cartao/credito/comprarCredito");
					modelAndView.addObject("erro", "O limite é menor que o valor da compra!");
					return modelAndView;
				}
				//realizar compra
			}else {
				// senha invalida
				modelAndView.setViewName("cartao/credito/comprarCredito");
				modelAndView.addObject("erro", "Senha invalida!");
				return modelAndView;
			}
			
		} else {
			modelAndView.addObject("erro", "Houve um erro ao efetuar compra!");
			modelAndView.setViewName("cartao/credito/comprarCredito");
			return modelAndView;
		}
	}
	
	
	@GetMapping("consultar")
    public ModelAndView chamaConsulta( ModelAndView modelAndView) {
		System.err.println("entrou consulta");
    	Conta conta = contaService.getConta(Const.ID_CONTA_LOGADA);
    	modelAndView.addObject("conta",conta);
    	modelAndView.setViewName("cartao/credito/consultarCredito");
    	System.err.println("SIZE COMPRAS "+conta.getCredito().getCompras().size());
		return modelAndView;
    }
	
	@PostMapping("pagarfatura")
	public ModelAndView pagarfatura( ModelAndView modelAndView) {
		System.err.println("Tentando pagar fatura");
			Conta conta = contaService.getConta(Const.ID_CONTA_LOGADA);
			System.err.println("ENTROU NO PAGEMENTO");

			if(conta.getSaldo() >= conta.getCredito().getValorFatura() && conta.getCredito().getValorFatura() > 0.0) {
				conta.setSaldo(conta.getSaldo() - conta.getCredito().getValorFatura());
				conta.getCredito().setLimite(conta.getCredito().getLimite() + conta.getCredito().getValorFatura());
				conta.getCredito().setValorFatura(0.0);
				contaService.createConta(conta);
				modelAndView.setViewName("cartao/credito/consultarCredito");
				modelAndView.addObject("conta",conta);
				modelAndView.addObject("ok", "Fatura paga com sucesso!");
				return modelAndView;
			}else {
				// senha invalida
				modelAndView.setViewName("cartao/credito/consultarCredito");
				modelAndView.addObject("conta",conta);
				modelAndView.addObject("erro", "Não possui saldo pra pagar a fatura!");
				return modelAndView;
			}
	}
	@PostMapping("seguros")
	public ModelAndView seguro( ModelAndView modelAndView,
			RedirectAttributes attributes, CartaoCredito credito, BindingResult result) {
		System.err.println("ANTES DO IF");
		if (!result.hasErrors()) {
			Conta conta = contaService.getConta(Const.ID_CONTA_LOGADA);
			System.err.println("ENTROU NO CADASTRA");
			credito.setAtivo(true);
			credito.setNumero(Utils.geraBlocosNumeros(4));
			credito.setLimite(conta.getCliente().getTipo().getLimite());
			conta.setCredito(credito);
			contaService.createConta(conta);
			modelAndView.setViewName("cartao/credito/cartaoCredito");
			modelAndView.addObject("ok", "Cartao de credito criado com limite aprovado de "+credito.getLimite());
			cartaoCreditoService.verificaCartao(modelAndView, conta);
			return modelAndView;
		} else {
			modelAndView.addObject("erro", "Houve um erro no cartao!");
			modelAndView.setViewName("cartao/credito/cartaoCredito");
			return modelAndView;
		}
	}
	@GetMapping("seguros")
    public ModelAndView chamaSeguro( ModelAndView modelAndView,CartaoCredito credito) {
		System.err.println("chamou seguro");
    	Conta conta = contaService.getConta(Const.ID_CONTA_LOGADA);
    	modelAndView.addObject("conta",conta);
    	modelAndView.setViewName("cartao/credito/criarCredito");
		return modelAndView;
    }
	
	
	
}