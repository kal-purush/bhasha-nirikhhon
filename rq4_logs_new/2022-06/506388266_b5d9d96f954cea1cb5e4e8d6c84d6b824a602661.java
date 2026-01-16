package br.com.dev.wakandaacademy.produdoro.credencial.application.service;

import javax.validation.Valid;

import br.com.dev.wakandaacademy.produdoro.usuario.application.api.UsuarioNovoRequest;

public interface CredencialApllicationService {

	void criaNovaCredencial(@Valid UsuarioNovoRequest usuarioNovo);

}