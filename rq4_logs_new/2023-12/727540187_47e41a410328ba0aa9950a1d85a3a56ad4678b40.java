package com.team4.payroll.dto;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotBlank;
import lombok.Data;

@Data
public class CreateEmpleadoDTO {

    @Schema(description = "Nombre del empleado", example = "Juan")
    @NotBlank
    private String nombre;
    @Schema(description = "Apellido del empleado", example = "Perez")
    @NotBlank
    private String apellido;
    @Schema(description = "Email del empleado", example = "juan@email.com")
    @NotBlank(message = "El email no puede estar vacío")
    private String email;
    
}