package br.com.fiap.segurossaas.repository;

 import org.springframework.data.repository.CrudRepository;

import br.com.fiap.segurossaas.model.entity.Segurado;

public interface SeguradoRepository extends CrudRepository<Segurado, Long> {
}