package com.example.taskmanager.repository;

import com.example.taskmanager.model.Prioridade;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.stereotype.Repository;

import java.util.Optional;

@Repository
public interface PrioridadeRepository extends JpaRepository<Prioridade, Long> {

    @Query("SELECT p FROM Prioridade p WHERE LOWER(p.texto) = LOWER(:texto)")
    Optional<Prioridade> findByTextoIgnoreCase(String texto);
}