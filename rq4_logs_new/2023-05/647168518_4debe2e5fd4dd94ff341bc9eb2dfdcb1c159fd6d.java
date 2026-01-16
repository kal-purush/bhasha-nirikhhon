package com.pruebatecnica.sanitas.models;

import lombok.*;

import java.math.BigDecimal;
import java.util.List;


@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class Operandos {

    private List<BigDecimal> operandos;

}