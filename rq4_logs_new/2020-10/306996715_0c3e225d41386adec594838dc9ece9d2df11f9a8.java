package br.com.pi.sebovirtual.controllers;

import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import br.com.pi.sebovirtual.entities.Avaliacao;
import br.com.pi.sebovirtual.repositories.AvaliacaoRepository;
import br.com.pi.sebovirtual.resource.BaseController;
import br.com.pi.sebovirtual.services.AvaliacaoService;

@RestController
@RequestMapping("avaliacao")
public class AvaliacaoController extends BaseController<Avaliacao, AvaliacaoRepository, AvaliacaoService> {

}