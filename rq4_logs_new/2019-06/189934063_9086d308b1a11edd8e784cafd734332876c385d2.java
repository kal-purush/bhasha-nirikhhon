package coop.tecso.examen.repository;

import org.springframework.data.jpa.repository.JpaRepository;

import coop.tecso.examen.model.PersonaFisica;

public interface PersonaFisicaRepository extends JpaRepository<PersonaFisica, Long> {

}