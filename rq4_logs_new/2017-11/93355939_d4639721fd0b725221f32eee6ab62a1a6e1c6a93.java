package ar.com.utn.proyecto.qremergencias.core.dto;

import lombok.Data;

import java.time.LocalDate;

@Data
public class FilterDTO {

    private LocalDate from;
    private LocalDate to;
    private String text;

}