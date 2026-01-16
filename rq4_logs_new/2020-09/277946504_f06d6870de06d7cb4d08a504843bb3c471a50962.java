package com.beltran.gestionempleados.service;

import com.beltran.gestionempleados.domain.Fichajes;
import com.beltran.gestionempleados.domain.Usuarios;
import com.beltran.gestionempleados.repository.FichajesRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Optional;

@Service
public class FichajeService {

    @Autowired
    private FichajesRepository fichajeRepo;

    public String verificarUltimaAccion(Usuarios usuario) {

        String accion ="";

        Optional<Fichajes> ultimaAccion = fichajeRepo.findTopByUsuarioOrderByIdDesc(usuario);

        if(ultimaAccion.isPresent()) {
            if (ultimaAccion.get().getAccion().equals("Ingreso")) {
                accion = "Egreso";
            } else if (ultimaAccion.get().getAccion().equals("Egreso")) {
                accion = "Ingreso";
            }
            return accion;
        }else {
            accion = "Ingreso";
        }
        return accion;
    }
}


