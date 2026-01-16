package com.thefilipov.food.filipovfood.client;

import com.thefilipov.food.filipovfood.client.api.ClientApiException;
import com.thefilipov.food.filipovfood.client.api.RestauranteClient;
import com.thefilipov.food.filipovfood.client.model.RestauranteModel;
import com.thefilipov.food.filipovfood.client.model.input.CidadeIdInput;
import com.thefilipov.food.filipovfood.client.model.input.CozinhaIdInput;
import com.thefilipov.food.filipovfood.client.model.input.EnderecoInput;
import com.thefilipov.food.filipovfood.client.model.input.RestauranteInput;
import lombok.extern.log4j.Log4j2;
import org.springframework.web.client.RestTemplate;

import java.math.BigDecimal;

@Log4j2
public class InclusaoRestauranteMain {

    public static void main(String[] args) {
        try {
            var restTemplate = new RestTemplate();
            var restauranteClient = new RestauranteClient(
                    restTemplate, "http://api.filipov-food.local:8082");

            var cozinha = new CozinhaIdInput();
            cozinha.setId(1L);

            var cidade = new CidadeIdInput();
            cidade.setId(1L);

            var endereco = new EnderecoInput();
            endereco.setCidade(cidade);
            endereco.setCep("38500-111");
            endereco.setLogradouro("Rua Xyz");
            endereco.setNumero("300");
            endereco.setBairro("Centro");

            var restaurante = new RestauranteInput();
            restaurante.setNome("Comida Mineira");
            restaurante.setTaxaFrete(new BigDecimal(9.5));
            restaurante.setCozinha(new CozinhaIdInput());
            restaurante.setCozinha(cozinha);
            restaurante.setEndereco(endereco);

            RestauranteModel restauranteModel = restauranteClient.adicionar(restaurante);

            log.info(restauranteModel);
        } catch (ClientApiException e) {
            if (e.getProblem() != null) {
                log.error(e.getProblem().getUserMessage());

                e.getProblem().getObjects().stream()
                        .forEach(p -> System.out.println("- " + p.getUserMessage()));

            } else {
                log.error("Erro desconhecido");
                e.printStackTrace();
            }
        }
    }
}