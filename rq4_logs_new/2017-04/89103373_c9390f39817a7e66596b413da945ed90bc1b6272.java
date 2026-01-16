package br.com.api.controller;

import java.util.HashMap;
import java.util.Map;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.google.gson.Gson;

import br.com.api.enums.StatusCliente;
import br.com.api.enums.StatusViagem;
import br.com.api.vo.ClienteVO;
import br.com.api.vo.RequestViagemVO;

@RestController
@RequestMapping("viagem")
public class ViagensController {
		
	
	@ResponseBody
	@RequestMapping(method=RequestMethod.POST,produces="application/json; charset=UTF-8")
	public ResponseEntity<String> realizaAbertura(@RequestBody RequestViagemVO requestViagemVO){
		
		Map<String, Object> retorno = new HashMap<String, Object>();
		Gson gson = new Gson();
		
		System.out.println(requestViagemVO.getCliente().getNome());
		System.out.println(requestViagemVO.getPagamento().getForma_de_pagamento());
		System.out.println(requestViagemVO.getDestinoViagem().getNomeDestino());
		
		requestViagemVO.setStatusViagem(StatusViagem.ACEITA);
		
		ClienteVO clienteVO = new ClienteVO();
		clienteVO.setCpf(requestViagemVO.getCliente().getCpf());
		clienteVO.setNome(requestViagemVO.getCliente().getNome());
		clienteVO.setEstadoCivil(requestViagemVO.getCliente().getEstadoCivil());
		clienteVO.setStatusCliente(StatusCliente.ACEITO);
		
		requestViagemVO.setCliente(clienteVO);
		
		retorno.put("retorno", requestViagemVO);
		
		return new ResponseEntity<String>(gson.toJson(retorno), HttpStatus.OK);
	}
	
}