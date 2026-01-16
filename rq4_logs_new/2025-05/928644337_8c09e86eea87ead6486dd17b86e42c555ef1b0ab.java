package br.com.mercado.domain.mapper;

import br.com.mercado.domain.dto.request.CategoriaRequest;
import br.com.mercado.domain.dto.request.MercadoRequest;
import br.com.mercado.domain.dto.response.CategoriaResponse;
import br.com.mercado.domain.model.Categoria;
import br.com.mercado.domain.model.Mercado;

public class CategoriaMapper {

    public static Categoria toEntity(CategoriaRequest request){
        return Categoria.builder()
                .nome(request.getNome())
                .build();
    }

    public static CategoriaResponse toResponse(Categoria categoria){
        return CategoriaResponse.builder()
                .nome(categoria.getNome())
                .build();
    }



}