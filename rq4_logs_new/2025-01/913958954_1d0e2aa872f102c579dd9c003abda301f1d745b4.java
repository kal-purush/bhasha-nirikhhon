package com.api.cep.entity;

import java.time.LocalDateTime;
import java.util.Date;

import com.fasterxml.jackson.annotation.JsonProperty;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Temporal;
import jakarta.persistence.TemporalType;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;

@Entity
@Getter
@Setter
@AllArgsConstructor
public class Usuarios {
    
    @Id
	@GeneratedValue(strategy = GenerationType.IDENTITY)
	private Long id;

    @Column(nullable = false)
    private String nome;

    @Column(nullable = false, unique = true)
    private String cpf;
    
    @Column(nullable = false)
    private String cep;
    
    private String logradouro;
    
    private String bairro;
    
    private String cidade;

    private String estado;

	@Column(name = "data_criacao", nullable = false, updatable = false)
	@JsonProperty("data_criacao")
	private LocalDateTime dataCriacao;

	@Column(name = "data_atualizacao", nullable = true)
	private LocalDateTime dataAtualizacao;
}