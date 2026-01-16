package com.example.demo.service;

import java.util.UUID;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.stereotype.Service;

import com.example.demo.controller.dtos.AgendaRequest;
import com.example.demo.domain.Agendamento;
import com.example.demo.repository.AgendamentoRepository;

@Service
public class AgendamentoService {

	@Autowired
	private AgendamentoRepository repository;

	public Agendamento getOne(UUID id) {
		return repository.getOne(id);
	}

	public Page<Agendamento> getAll(int page, int size) {
		return repository.findAll(PageRequest.of(page, size));
	}

	public Agendamento create(Agendamento agendamento) {
		return repository.save(agendamento);
	}

	public Agendamento update(UUID id, AgendaRequest agendamento) {
		Agendamento a = repository.getOne(id);

		a.setEmail(agendamento.getEmail());
		a.setNome(agendamento.getNome());
		a.setTelefone(agendamento.getTelefone());
		a.setDataAgendamento(agendamento.getDataAgendamento());

		return repository.save(a);
	}

	public void delete(UUID id) {
		Agendamento a = repository.getOne(id);
		repository.delete(a);
	}

}