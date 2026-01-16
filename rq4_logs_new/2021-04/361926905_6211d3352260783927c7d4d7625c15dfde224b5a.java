package com.agustincharry.reactivewebtest.client.services;

import com.agustincharry.reactivewebtest.models.Car;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.Mock;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.util.ArrayList;
import java.util.List;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

@SpringBootTest()
public class CarServiceClientTests {

    @Mock
    private WebClient webClientMock;

    @Mock
    private WebClient.RequestHeadersSpec requestHeadersSpecMock;

    @Mock
    private WebClient.RequestHeadersUriSpec requestHeadersUriSpecMock;

    @Mock
    private WebClient.ResponseSpec responseSpecMock;

    @Autowired
    private CarServiceClient service;

    private final List<Car> list;

    public CarServiceClientTests() {
        list = new ArrayList<>();
        list.add(new Car("1", "Mazda", "red", "2"));
        list.add(new Car("2", "Renault", "White", "1"));
        list.add(new Car("a", "a", "a", "a"));
        list.add(new Car("b", "b", "b", "b"));
    }

    @Test
    public void getAll(){
        ReflectionTestUtils.setField(service, "webClient", webClientMock);
        when(webClientMock.get()).thenReturn(requestHeadersUriSpecMock);
        when(requestHeadersUriSpecMock.uri(anyString())).thenReturn(requestHeadersSpecMock);
        when(requestHeadersSpecMock.retrieve()).thenReturn(responseSpecMock);
        when(responseSpecMock.bodyToFlux(ArgumentMatchers.<Class<Car>>notNull())).thenReturn(Flux.fromIterable(list));

        Flux<Car> result = service.getAll();

        StepVerifier.create(result)
                .expectNextSequence(list)
                .verifyComplete();

        StepVerifier.create(result)
                .expectSubscription()
                .expectNextCount(list.size())
                .verifyComplete();
    }

    @Test
    public void getById() {
        ReflectionTestUtils.setField(service, "webClient", webClientMock);
        when(webClientMock.get()).thenReturn(requestHeadersUriSpecMock);
        when(requestHeadersUriSpecMock.uri(anyString())).thenReturn(requestHeadersSpecMock);
        when(requestHeadersSpecMock.retrieve()).thenReturn(responseSpecMock);

        for (Car car : list) {
            when(responseSpecMock.bodyToMono(ArgumentMatchers.<Class<Car>>notNull())).thenReturn(Mono.just(car));

            Mono<Car> carFlux = service.getById(car.getId());

            StepVerifier.create(carFlux)
                    .expectNext(car)
                    .verifyComplete();

            StepVerifier.create(carFlux)
                    .expectSubscription()
                    .expectNextCount(1)
                    .verifyComplete();
        }
    }

}