package ru.task.deliveryapp.infrastructure.adapters.postgres;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import ru.task.deliveryapp.core.domain.aggregate.courier.Courier;
import ru.task.deliveryapp.core.domain.aggregate.courier.CourierStatus;
import ru.task.deliveryapp.core.ports.CourierRepository;
import ru.task.deliveryapp.exception.DbException;
import ru.task.deliveryapp.infrastructure.adapters.postgres.repository.CourierJpaRepository;

import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

@Component
public class CourierAdapter implements CourierRepository {

    @Autowired
    CourierJpaRepository repository;

    @Override
    public Courier add(Courier courier) {
        if ((courier.getId() == null) || repository.findById(courier.getId()).isEmpty()) {
            return CourierMapper.toDomain(repository.save(CourierMapper.toEntity(courier)));
        }
        else {
            throw new DbException("A new courier cannot be added because it exists already");
        }
    }

    @Override
    public void update(Courier courier) {
        if (courier.getId() != null && !repository.findById(courier.getId()).isEmpty()) {
            repository.save(CourierMapper.toEntity(courier));
        }
        else {
            throw new DbException("A courier cannot be updates because it doesn't exist");
        }
    }

    @Override
    public Courier get(UUID courierId) {
        return CourierMapper.toDomain(repository.findById(courierId).get());
    }

    @Override
    public List<Courier> getAllReady() {
        return repository.findByStatus(CourierStatus.READY).stream().map(entity -> CourierMapper.toDomain(entity)).collect(Collectors.toList());
    }

    @Override
    public List<Courier> getAllBusy() {
        return repository.findByStatus(CourierStatus.BUSY).stream().map(entity -> CourierMapper.toDomain(entity)).collect(Collectors.toList());
    }
}