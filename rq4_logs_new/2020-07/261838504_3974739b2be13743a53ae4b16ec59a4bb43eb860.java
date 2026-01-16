package com.ivo.rakar.foodapp.consumerservice.domain;

import io.eventuate.tram.events.publisher.ResultWithEvents;

import javax.persistence.*;

@Entity
@Table(name = "consumers")
@Access(AccessType.FIELD)
public class Consumer {

    @Id
    @GeneratedValue
    private Long id;

    @Embedded
    private PersonName personName;

    public Consumer(PersonName personName) {
        this.personName = personName;
    }

    private Consumer() {}

    public void validateOrderByConsumer(int orderTotal) {

    }

    public static ResultWithEvents<Consumer> create(PersonName personName) {
        return new ResultWithEvents<Consumer>(new Consumer(personName), new ConsumerCreated());
    }

    public Long getId() {
        return id;
    }

    public PersonName getPersonName() {
        return personName;
    }

}