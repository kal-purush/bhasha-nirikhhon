package com.example.ApiRestFulSpringBoot.domain.dto;

import com.example.ApiRestFulSpringBoot.domain.Carro;
import lombok.Data;

@Data
public class CarroDTO
{
    private Long id;
    private String nome;
    private String tipo;

    public CarroDTO(Carro c)
    {
        this.id = c.getId();
        this.nome = c.getNome();
        this.tipo =c.getTipo();
    }


}