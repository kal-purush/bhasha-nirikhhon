package com.api.financas.dto;

import com.api.financas.domain.receita.Receita;
import jakarta.validation.constraints.NotNull;

import java.time.LocalDate;
import java.util.UUID;

public record ReceitaResponseDTO(@NotNull UUID id, @NotNull LocalDate data, @NotNull Double saldoLiquido,
                                 @NotNull Double rendimentoAluguel,
                                 @NotNull Double rendimentoAplicacoes,
                                 @NotNull Double outros) {
    public ReceitaResponseDTO(Receita receita) {
        this(receita.getId(),
                receita.getData(),
                receita.getSaldoLiquido(),
                receita.getRendimentoAluguel(),
                receita.getRendimentoAplicacoes(),
                receita.getOutros()
        );
    }
}