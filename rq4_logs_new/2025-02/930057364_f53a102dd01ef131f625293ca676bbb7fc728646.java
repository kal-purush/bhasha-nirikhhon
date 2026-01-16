package com.sub.veiculo.infrastructure.adapter.persistence;

import com.sub.veiculo.domain.model.Vehicle;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class VehicleRepositoryAdapterTest {

    @Mock
    private JpaVehicleRepository jpaRepository;

    @InjectMocks
    private VehicleRepositoryAdapter adapter;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
    }

    @Test
    void testSave() {
        Vehicle vehicle = new Vehicle(1L, "Ford", "Focus", 2022, "Preto", 75000.0);
        VehicleEntity entity = new VehicleEntity();
        entity.setId(1L);
        entity.setMarca("Ford");
        entity.setModelo("Focus");
        entity.setAno(2022);
        entity.setCor("Preto");
        entity.setPreco(75000.0);

        when(jpaRepository.save(any(VehicleEntity.class))).thenReturn(entity);

        Vehicle savedVehicle = adapter.save(vehicle);

        assertNotNull(savedVehicle);
        assertEquals(vehicle.getId(), savedVehicle.getId());
        assertEquals(vehicle.getMarca(), savedVehicle.getMarca());
        verify(jpaRepository, times(1)).save(any(VehicleEntity.class));
    }

    @Test
    void testFindById_WhenVehicleExists() {
        VehicleEntity entity = new VehicleEntity();
        entity.setId(1L);
        entity.setMarca("Ford");
        entity.setModelo("Focus");
        entity.setAno(2022);
        entity.setCor("Preto");
        entity.setPreco(75000.0);

        when(jpaRepository.findById(1L)).thenReturn(Optional.of(entity));

        Optional<Vehicle> result = adapter.findById(1L);

        assertTrue(result.isPresent());
        assertEquals(entity.getId(), result.get().getId());
        verify(jpaRepository, times(1)).findById(1L);
    }

    @Test
    void testFindById_WhenVehicleDoesNotExist() {
        when(jpaRepository.findById(1L)).thenReturn(Optional.empty());

        Optional<Vehicle> result = adapter.findById(1L);

        assertFalse(result.isPresent());
        verify(jpaRepository, times(1)).findById(1L);
    }

    @Test
    void testFindAll() {
        VehicleEntity entity1 = new VehicleEntity();
        entity1.setId(1L);
        entity1.setMarca("Ford");
        entity1.setModelo("Focus");
        entity1.setAno(2022);
        entity1.setCor("Preto");
        entity1.setPreco(75000.0);

        VehicleEntity entity2 = new VehicleEntity();
        entity2.setId(2L);
        entity2.setMarca("Chevrolet");
        entity2.setModelo("Cruze");
        entity2.setAno(2021);
        entity2.setCor("Branco");
        entity2.setPreco(85000.0);

        when(jpaRepository.findAll()).thenReturn(Arrays.asList(entity1, entity2));

        List<Vehicle> vehicles = adapter.findAll();

        assertNotNull(vehicles);
        assertEquals(2, vehicles.size());
        assertEquals(entity1.getId(), vehicles.get(0).getId());
        assertEquals(entity2.getId(), vehicles.get(1).getId());
        verify(jpaRepository, times(1)).findAll();
    }

    @Test
    void testDeleteById() {
        Long id = 1L;
        doNothing().when(jpaRepository).deleteById(id);

        adapter.deleteById(id);

        verify(jpaRepository, times(1)).deleteById(id);
    }
}