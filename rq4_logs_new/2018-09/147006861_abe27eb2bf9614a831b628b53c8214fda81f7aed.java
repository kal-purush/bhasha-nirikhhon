package com.pw.sb.demo.generator;

import com.pw.sb.demo.model.Person;
import reactor.core.publisher.Flux;

public class FluxGenerator<T> {

    private Class<T> classOfGenericType;
    private int noOfObjects;

    public FluxGenerator(final Class<T> classOfGenericType, final int noOfObjects){
        this.classOfGenericType = classOfGenericType;
        this.noOfObjects = noOfObjects;
    }

    public Flux<T> generate(){
        final ModelGenerator<T> generator = new ModelGenerator<>(classOfGenericType);

        return Flux.range(0, noOfObjects)
                .map(nr -> generator.generate());
    }
}