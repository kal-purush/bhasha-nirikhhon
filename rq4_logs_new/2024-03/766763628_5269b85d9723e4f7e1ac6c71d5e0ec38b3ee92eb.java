package co.edu.icesi.viajes.icesiviajes.domain;

import co.edu.icesi.viajes.icesiviajes.repository.DestinoRepository;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.core.AutoConfigureCache;
import org.springframework.boot.test.context.SpringBootTest;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest
class DestinoTest {

    @Autowired
    private DestinoRepository destinoRepository;

    @Test
    void retornarTotalClientePorEstado(){
        List<Destino> destinos = destinoRepository.consultarDestinoPorEstado("A","A");
        for (Destino destino : destinos){
            System.out.println(destino.getNombre());
        }
    }

    @Test
    void findByIdTide(){
        List<Destino> destinos = destinoRepository.findByIdTide(1);
        for (Destino destino : destinos){
            System.out.println(destino.getNombre());
        }
    }

    @Test
    void consultarDestinosActivos(){
        List<Destino> destinos = destinoRepository.consultarDestinosActivos("A");
        for (Destino destino : destinos){
            System.out.println(destino.getNombre());
        }
    }
}