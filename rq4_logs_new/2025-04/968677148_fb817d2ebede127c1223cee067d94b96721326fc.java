package com.unifucamp.gamearena.domain;

import java.time.LocalDate;
import java.time.LocalDateTime;

import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import lombok.Getter;
import lombok.Setter;

@Entity
@Getter
@Setter
public class Inscricao {

  @Id
  private Long id;

  private String nomeCompleto;

  private String nickname;

  private String email;

  private LocalDate dataNascimento;

  private String comprovantePagamento;

  private boolean aceitouTermos;

  private LocalDateTime dataInscricao;

  private boolean ativo;

  private boolean pagamento;
}