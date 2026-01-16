package com.example.ProjectReactor;

import reactor.core.publisher.Flux;

public class FluxMerger {
    public static Flux<String> mergeFluxes(Flux<String> flux1, Flux<String> flux2) {
        return Flux.merge(flux1, flux2);
    }

    public static void main(String[] args) {
        Flux<String> flux1 = Flux.just("A", "B", "C");
        Flux<String> flux2 = Flux.just("1", "2", "3");

        mergeFluxes(flux1, flux2)
                .subscribe(System.out::println);  // Output: A, 1, B, 2, C, 3
    }
}