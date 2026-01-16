package com.algaworks.algafood.api.assembler;

import com.algaworks.algafood.api.model.CozinhaDtoOutput;
import com.algaworks.algafood.api.model.RestauranteDtoOutput;
import com.algaworks.algafood.domain.model.Restaurante;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.stream.Collectors;

@Component
public class RestauranteModelAssembler {

    public List<RestauranteDtoOutput> toCollectionModel(List<Restaurante> restaurantes) {
        return restaurantes.stream().map(restaurante -> toModel(restaurante)).collect(Collectors.toList());
    }

    public RestauranteDtoOutput toModel(Restaurante restaurante) {
        CozinhaDtoOutput cozinhaDtoOutput = new CozinhaDtoOutput();
        cozinhaDtoOutput.setId(restaurante.getCozinha().getId());
        cozinhaDtoOutput.setNome(restaurante.getCozinha().getNome());

        RestauranteDtoOutput restauranteDtoOutput = new RestauranteDtoOutput();
        restauranteDtoOutput.setId(restaurante.getId());
        restauranteDtoOutput.setNome(restaurante.getNome());
        restauranteDtoOutput.setTaxaFrete(restaurante.getTaxaFrete());
        restauranteDtoOutput.setCozinha(cozinhaDtoOutput);
        return restauranteDtoOutput;
    }

}