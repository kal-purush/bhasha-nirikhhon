package br.com.pi.sebovirtual.controllers;

import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import br.com.pi.sebovirtual.entities.PessoaFisica;
import br.com.pi.sebovirtual.repositories.PessoaFisicaRepository;
import br.com.pi.sebovirtual.resource.BaseController;
import br.com.pi.sebovirtual.services.PessoaFisicaService;

@RestController
@RequestMapping("pessoafisica")
public class PessoaFisicaController extends BaseController
	<PessoaFisica, PessoaFisicaRepository, PessoaFisicaService> {

}