package com.icoffiel.games.db.kafka;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

/**
 * Table for capturing the system entities from the kafka topic related to the creation of systems (systems.sql.system_entity)
 */
@Entity
@Table(name = "kafka_systems.sql.system_entity")
public class KafkaSystemEntity {
    @Id
    @Column(name = "id", nullable = false)
    private Long id;

    @Column(name = "name", nullable = false)
    private String name;

    public void setId(Long id) {
        this.id = id;
    }

    public Long getId() {
        return id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }
}