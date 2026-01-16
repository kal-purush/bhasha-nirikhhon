package ru.task.deliveryapp.core.ports;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.orm.jpa.DataJpaTest;
import org.springframework.test.context.jdbc.Sql;
import ru.task.deliveryapp.core.domain.courieraggregate.Courier;

import java.util.Collection;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Интеграционный тест репозитория CourierRepository.
 */
@DataJpaTest
public class CourierRepositoryIT {
    @Autowired
    CourierRepository repository;

    @Test
    @Sql("courier_data.sql")
    public void testGetAllActive() {
        Collection<Courier> couriers = repository.getAllActive();
        assertEquals(2, couriers.size());
    }
}