package com.mggcode.gestion_bd_elecciones.DTO.autonomicas;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class SedesDTO {
    private String codigoPartido;
    private String codigoPadre;
    private int escanos_desde;
    private int escanos_hasta;
    private int escanos_hist;
    private double porcentajeVoto;
    private double porcentajeVotoHistorico;
    private int numVotantes;
    private String siglas;
    private String literalPartido;
    private int numVotantes_hist;
}