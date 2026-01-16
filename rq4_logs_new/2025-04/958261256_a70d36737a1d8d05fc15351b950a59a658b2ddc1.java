package org.openapitools.dto.ReporteDTO;

import jakarta.validation.constraints.*;
import lombok.Data;
import org.openapitools.dto.UbicacionDto;
import org.openapitools.model.enums.StatusReporte;

import java.util.List;

@Data
public class ReportePatchRequestDto {

    private int id;

    @Size(max = 255)
    private String titulo;

    private String descripcion;

    private UbicacionDto ubicacion;

    private List<@Pattern(regexp = "^(https?|ftp)://[^\\s/$.?#].[^\\s]*$") String> imagenes;

    private StatusReporte estado;

    private Integer contadorImportancia;
}
