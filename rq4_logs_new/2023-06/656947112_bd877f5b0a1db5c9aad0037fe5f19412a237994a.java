package com.ar.conde.reservaDeTurnos.repositories;

import static org.junit.jupiter.api.Assertions.*;

import java.util.Optional;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.orm.jpa.DataJpaTest;

import com.ar.conde.reservaDeTurnos.entity.Odontologo;


@DataJpaTest
public class IOdontologoRepositoryTest {

    @Autowired
    private IOdontologoRepository odontologoRepository;

    @Test
    public void testBuscarPorMatricula_Valido() {
        Odontologo odontologo = Odontologo.builder()
                .nombre("Nombre")
                .apellido("Apellido")
                .matricula(123456)
                .build();
        odontologoRepository.save(odontologo);

        Optional<Odontologo> resultado = odontologoRepository.buscarPorMatricula(123456);

        assertTrue(resultado.isPresent());
        assertEquals(odontologo.getMatricula(), resultado.get().getMatricula());
    }

    @Test
    public void testBuscarPorMatricula_Invalido() {
        Optional<Odontologo> resultado = odontologoRepository.buscarPorMatricula(123456);

        assertFalse(resultado.isPresent());
    }

    @Test
    public void testGuardarOdontologo() {
        Odontologo odontologo = Odontologo.builder()
                .nombre("Nombre")
                .apellido("Apellido")
                .matricula(123456)
                .build();

        Odontologo resultado = odontologoRepository.save(odontologo);

        assertEquals(odontologo.getNombre(), resultado.getNombre());
        assertEquals(odontologo.getApellido(), resultado.getApellido());
        assertEquals(odontologo.getMatricula(), resultado.getMatricula());
    }

}