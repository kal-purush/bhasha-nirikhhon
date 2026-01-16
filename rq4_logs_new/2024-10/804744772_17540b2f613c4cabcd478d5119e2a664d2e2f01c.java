package com.example.ProjectReactor;

import reactor.core.publisher.Flux;

public class ErrorHandling {
    public static Flux<String> handleErrors(Flux<String> flux) {
        return flux.doOnNext(item ->{
            if(item.equals("ERROR")){
                throw new IllegalArgumentException("ERROR encountered");
            }
        }).onErrorResume(e -> Flux.just("Error handled" + e.getMessage()));
    }
    public static void main(String args[]){
        Flux<String> fluxStream = Flux.just("Item1", "ERROR", "Item2");
        handleErrors(fluxStream).subscribe(System.out::println);
    }
}