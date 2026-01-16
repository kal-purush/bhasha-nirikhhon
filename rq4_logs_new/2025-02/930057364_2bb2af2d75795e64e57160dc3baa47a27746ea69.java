package com.sub.veiculo.application.service;

import com.sub.veiculo.application.port.input.VehicleUseCase;
import com.sub.veiculo.application.port.output.VehicleRepositoryPort;
import com.sub.veiculo.domain.model.Vehicle;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Optional;

@Service
public class VehicleService implements VehicleUseCase {
    private final VehicleRepositoryPort repository;

    public VehicleService(VehicleRepositoryPort repository) {
        this.repository = repository;
    }

    @Override
    public Vehicle createVehicle(Vehicle vehicle) {
        return repository.save(vehicle);
    }

    @Override
    public Vehicle getVehicle(Long id) {
        return repository.findById(id).orElse(null);
    }

    @Override
    public List<Vehicle> listVehicles() {
        return repository.findAll();
    }

    @Override
    public void removeVehicle(Long id) {
        repository.deleteById(id);
    }

    public Vehicle updateVehicle(Long id, Vehicle updatedVehicle) {
        Optional<Vehicle> existingVehicle = repository.findById(id);
        if (existingVehicle.isPresent()) {
            return repository.save(updatedVehicle);
        }
        return null;
    }
}