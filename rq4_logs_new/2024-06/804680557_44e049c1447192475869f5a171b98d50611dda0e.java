package com.ticket.ticketProject.model.dto.ingresso;

import com.ticket.ticketProject.model.EstadoIngresso;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;

import java.time.LocalDate;

public record AtualizarIngressoDTO(
        @NotNull
        Long id,

        String evento,

        LocalDate data,

        Double preco,

        String local,

        EstadoIngresso estado
) {
}