package com.thefilipov.food.filipovfood.client.api;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.thefilipov.food.filipovfood.client.model.Problem;
import lombok.Getter;
import lombok.extern.log4j.Log4j2;
import org.springframework.web.client.RestClientResponseException;

@Log4j2
public class ClientApiException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    @Getter
    private Problem problem;
    public ClientApiException(String message, RestClientResponseException cause) {
        super(message, cause);

        //log.error(cause.getResponseBodyAsString());
        deserializeProblem(cause);
    }

    private void deserializeProblem(RestClientResponseException cause) {
        ObjectMapper mapper = new ObjectMapper();
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        mapper.registerModule(new JavaTimeModule());
        mapper.findAndRegisterModules();

        try {
            this.problem = mapper.readValue(cause.getResponseBodyAsString(), Problem.class);
        } catch (JsonProcessingException e) {
            log.warn("Não foi possível desserializar a resposta em um problema", e);
        }
    }

}