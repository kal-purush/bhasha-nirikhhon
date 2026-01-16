package com.ar.conde.reservaDeTurnos.service;

import com.ar.conde.reservaDeTurnos.entity.Odontologo;
import com.ar.conde.reservaDeTurnos.repositories.IOdontologoRepository;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static org.mockito.Mockito.*;

class OdontologoServiceTest {

    private OdontologoService odontologoService;

    @Mock
    private IOdontologoRepository odontologoRepository;

    @BeforeEach
    public void setup() {
        MockitoAnnotations.initMocks(this);
        odontologoService = new OdontologoService(odontologoRepository);
    }

    @Test
    public void testGetAll() {
        List<Odontologo> odontologos = new ArrayList<>();
        odontologos.add(new Odontologo("Nombre1", "Apellido1", 12345));
        odontologos.add(new Odontologo("Nombre2", "Apellido2", 54321));

        when(odontologoRepository.findAll()).thenReturn(odontologos);

        List<Odontologo> result = odontologoService.getAll();

        Assertions.assertEquals(2, result.size());
        Assertions.assertEquals(odontologos, result);
        verify(odontologoRepository, times(1)).findAll();
    }

    @Test
    public void testGetById() {
        Long odontologoId = 1L;
        Odontologo odontologo = new Odontologo("Nombre", "Apellido", 12345);

        when(odontologoRepository.findById(odontologoId)).thenReturn(Optional.of(odontologo));

        Optional<Odontologo> result = odontologoService.getById(odontologoId);

        Assertions.assertTrue(result.isPresent());
        Assertions.assertEquals(odontologo, result.get());
        verify(odontologoRepository, times(1)).findById(odontologoId);
    }

    @Test
    public void testCreate() {
        Odontologo odontologo = new Odontologo("Nombre", "Apellido", 12345);

        when(odontologoRepository.buscarPorMatricula(odontologo.getMatricula())).thenReturn(Optional.empty());
        when(odontologoRepository.save(odontologo)).thenReturn(odontologo);

        Odontologo result = odontologoService.create(odontologo);

        Assertions.assertEquals(odontologo, result);
        verify(odontologoRepository, times(1)).buscarPorMatricula(odontologo.getMatricula());
        verify(odontologoRepository, times(1)).save(odontologo);
    }
}