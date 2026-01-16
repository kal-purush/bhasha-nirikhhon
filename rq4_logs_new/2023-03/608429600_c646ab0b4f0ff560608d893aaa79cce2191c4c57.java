package com.thefilipov.food.filipovfood.client;

import org.springframework.web.client.RestTemplate;

public class ListagemRestaurantesMain {

    public static void main(String[] args) {

        RestTemplate restTemplate = new RestTemplate();

        RestauranteClient restauranteClient = new RestauranteClient(
                restTemplate, "http://api.filipov-food.local:8082");

        restauranteClient.listar().stream()
                .forEach(restaurante -> System.out.println(restaurante));
    }
}