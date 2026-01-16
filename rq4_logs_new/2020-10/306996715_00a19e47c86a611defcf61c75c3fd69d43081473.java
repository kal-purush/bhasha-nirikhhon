package br.com.pi.sebovirtual.controllers;

import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import br.com.pi.sebovirtual.entities.HistoricoEndereco;
import br.com.pi.sebovirtual.repositories.HistoricoEnderecoRepository;
import br.com.pi.sebovirtual.resource.BaseController;
import br.com.pi.sebovirtual.services.HistoricoEnderecoService;

@RestController
@RequestMapping("/endereco")
public class HistoricoEnderecoController extends BaseController
	<HistoricoEndereco, HistoricoEnderecoRepository, HistoricoEnderecoService> {

}