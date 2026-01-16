package br.com.fiap.fiapfood.application.service;

import br.com.fiap.fiapfood.domain.model.RestauranteEntity;
import br.com.fiap.fiapfood.domain.repository.RestauranteRepository;
import br.com.fiap.fiapfood.infrastructure.persistence.JpaRestauranteRepository;
import org.springframework.stereotype.Service;

import java.util.Optional;

@Service
public class RestaurantRepositoryImp implements RestauranteRepository {

    private final JpaRestauranteRepository jpaRestauranteRepository;

    public RestaurantRepositoryImp(JpaRestauranteRepository jpaRestauranteRepository) {
        this.jpaRestauranteRepository = jpaRestauranteRepository;
    }

    @Override
    public RestauranteEntity salvar(RestauranteEntity restaurante) {
        return this.jpaRestauranteRepository.save(restaurante);
    }

    @Override
    public RestauranteEntity buscarPorId(Long id) {
        Optional<RestauranteEntity> restaurante = jpaRestauranteRepository.findById(id);
        if (restaurante.isPresent()) {
            return restaurante.get();
        }
        throw new RuntimeException("Restaurante não encontrado");
    }

    @Override
    public void deletar(Long id) {
        Optional<RestauranteEntity> restaurante = jpaRestauranteRepository.findById(id);
        if (restaurante.isPresent()) {
            jpaRestauranteRepository.delete(restaurante.get());
            return;
        }
        throw new RuntimeException("Restaurante não encontrado");
    }

    @Override
    public RestauranteEntity buscarPorNome(String nome) {
        Optional<RestauranteEntity> restaurante = jpaRestauranteRepository.findByNomeIsLike(nome);
        if (restaurante.isPresent()) {
            return restaurante.get();
        }
        throw new RuntimeException("Restaurante não encontrado");
    }

    @Override
    public RestauranteEntity buscarPorEndereco(String endereco) {
        Optional<RestauranteEntity> restaurante = jpaRestauranteRepository.findByEndereco(endereco);
        if (restaurante.isPresent()) {
            return restaurante.get();
        }
        throw new RuntimeException("Restaurante não encontrado");
    }

    @Override
    public RestauranteEntity buscarPorTipoCozinha(String tipoCozinha) {
        Optional<RestauranteEntity> restaurante = jpaRestauranteRepository.findByTipoCozinha(tipoCozinha);
        if (restaurante.isPresent()) {
            return restaurante.get();
        }
        throw new RuntimeException("Restaurante não encontrado");
    }
}