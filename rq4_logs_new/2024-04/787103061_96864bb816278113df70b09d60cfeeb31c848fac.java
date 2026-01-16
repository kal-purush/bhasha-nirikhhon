package br.com.fiap.fiapfood.application.usecase;

import br.com.fiap.fiapfood.domain.entity.RestauranteDomain;
import br.com.fiap.fiapfood.domain.gateway.RestauranteGateway;
import br.com.fiap.fiapfood.domain.usecase.RestauranteUsecase;
import lombok.AllArgsConstructor;
import org.springframework.stereotype.Component;

@Component
@AllArgsConstructor
public class RestauranteUseCaseImp implements RestauranteUsecase {

    private final RestauranteGateway restauranteGateway;

    @Override
    public RestauranteDomain salvar(RestauranteDomain restaurante) {
        return restauranteGateway.salvar(restaurante);
    }

    @Override
    public RestauranteDomain buscarPorId(Long id) {
        return restauranteGateway.buscarPorId(id);
    }

    @Override
    public void apagar(Long id) {
        restauranteGateway.apagar(id);
    }

    @Override
    public RestauranteDomain buscarPorNome(String nome) {
        return restauranteGateway.buscarPorNome(nome);
    }

    @Override
    public RestauranteDomain buscarPorEndereco(String endereco) {
        return restauranteGateway.buscarPorEndereco(endereco);
    }

    @Override
    public RestauranteDomain buscarPorTipoCozinha(String tipoCozinha) {
        return restauranteGateway.buscarPorTipoCozinha(tipoCozinha);
    }
}