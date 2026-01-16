package com.br.estudo.webflux.controller.dto;

import java.time.Instant;

public record MetaDTO(
        Instant requestDateTime,
        int totalRecords,
        int totalPages
) { }