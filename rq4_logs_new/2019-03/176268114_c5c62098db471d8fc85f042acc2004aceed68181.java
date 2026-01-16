package com.example.demo.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.example.demo.controller.dtos.AgendaRequest;
import com.example.demo.domain.Agendamento;
import com.example.demo.repository.AgendamentoRepository;

@RestController
@RequestMapping("/agendamento")
public class AgendamentoController {
	
	@Autowired
	private AgendamentoRepository repository;
	
	@PostMapping()
    public Agendamento create(@RequestBody Agendamento agendamento){
        return repository.save(agendamento);
    }

}