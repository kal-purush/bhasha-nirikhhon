package com.example.demo.dto;

import com.example.demo.modelo.Producto;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.math.BigDecimal;

@Getter
@Setter
@NoArgsConstructor
public class ProductoDTO {
    Long id;
    String name;
    BigDecimal price;
    Long usuarioId;

    /**
     * El constructor coge un
     * objeto Producto y crea un ProductoDTO a partir de él.
     * Después copia el id, el nombre y el precio del producto al DTO,
     * y también obtiene el id del usuario asociado al producto.
     * @param producto
     */
    public ProductoDTO(Producto producto) {
        id = producto.getId();
        name = producto.getName();
        price = producto.getPrice();
        usuarioId = producto.getUsuario().getId();
    }
}