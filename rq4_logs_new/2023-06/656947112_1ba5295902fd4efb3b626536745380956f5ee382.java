package com.ar.conde.reservaDeTurnos.repositories;

import com.ar.conde.reservaDeTurnos.entity.Domicilio;
import com.ar.conde.reservaDeTurnos.entity.Paciente;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.orm.jpa.DataJpaTest;
import org.springframework.boot.test.autoconfigure.orm.jpa.TestEntityManager;

import java.time.LocalDate;
import java.util.Optional;

@DataJpaTest
public class IPacienteRepositoryTest {

    @Autowired
    private TestEntityManager entityManager;
    @Autowired
    private IPacienteRepository pacienteRepository;
    @Test
    public void buscarPorDni_DebeRetornarPacienteCorrecto() {
        Domicilio domicilio = new Domicilio(null, "Calle 123", "456", "Localidad", "Provincia");
        Paciente paciente = new Paciente(null, domicilio, "Nombre", "Apellido", 12345678, LocalDate.now());
        entityManager.persistAndFlush(paciente);

        Optional<Paciente> pacienteEncontrado = pacienteRepository.buscarPorDni(paciente.getDni());

        Assertions.assertTrue(pacienteEncontrado.isPresent());
        Assertions.assertEquals(paciente.getDni(), pacienteEncontrado.get().getDni());
    }

    @Test
    public void buscarPorDni_DebeRetornarVacioCuandoNoExistePacienteConDni() {
        int dniInexistente = 99999999;

        Optional<Paciente> pacienteEncontrado = pacienteRepository.buscarPorDni(dniInexistente);

        Assertions.assertFalse(pacienteEncontrado.isPresent());
    }
}