package com.mggcode.gestion_bd_elecciones.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.socket.config.annotation.EnableWebSocket;
import org.springframework.web.socket.config.annotation.WebSocketConfigurer;
import org.springframework.web.socket.config.annotation.WebSocketHandlerRegistry;

@Configuration
@EnableWebSocket
public class WebSocketConfig implements WebSocketConfigurer {
    @Override
    public void registerWebSocketHandlers(WebSocketHandlerRegistry registry) {
        registry.addHandler(autonomicasCircunscripcionesHandler(), "autonomicas/updates");
        /*
        registry.addHandler(circunscripcionesHandler(), "municipales/circunscripciones/updates");
        registry.addHandler(circunscripcionesPartidosHandler(), "municipales/cp/updates");
        registry.addHandler(carmenDtoHandler(), "municipales/carmen/updates");

        registry.addHandler(autonomicasCircunscripcionesPartidosHandler(), "autonomicas/cp/updates");
        registry.addHandler(autonomicasCarmenDtoHandler(), "autonomicas/carmen/updates");*/

    }

    @Bean
    public WebSocketHandler circunscripcionesHandler() {
        return new WebSocketHandler("circunscripciones");
    }

    @Bean
    public WebSocketHandler circunscripcionesPartidosHandler() {
        return new WebSocketHandler("circunscripcionesPartidos");
    }

    @Bean
    public WebSocketHandler carmenDtoHandler() {
        return new WebSocketHandler("carmenDto");
    }

    @Bean
    public WebSocketHandler autonomicasCircunscripcionesHandler() {
        return new WebSocketHandler("autonomicasCircunscripciones");
    }

    @Bean
    public WebSocketHandler autonomicasCircunscripcionesPartidosHandler() {
        return new WebSocketHandler("autonomicasCircunscripcionesPartidos");
    }

    @Bean
    public WebSocketHandler autonomicasCarmenDtoHandler() {
        return new WebSocketHandler("autonomicasCarmenDto");
    }
}