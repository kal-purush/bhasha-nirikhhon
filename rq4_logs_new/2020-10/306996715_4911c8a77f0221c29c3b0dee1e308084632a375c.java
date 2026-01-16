package br.com.pi.sebovirtual.controllers;

import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import br.com.pi.sebovirtual.entities.Condicao;
import br.com.pi.sebovirtual.repositories.CondicaoRepository;
import br.com.pi.sebovirtual.resource.BaseController;
import br.com.pi.sebovirtual.services.CondicaoService;

@RestController
@RequestMapping("condicao")
public class CondicaoController extends BaseController
	<Condicao, CondicaoRepository, CondicaoService>{

}