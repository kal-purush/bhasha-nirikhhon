package pe.edu.upc.project_security_g06.servicesimplements;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import pe.edu.upc.project_security_g06.entities.Ciudad;
import pe.edu.upc.project_security_g06.repositories.ICiudadRepository;
import pe.edu.upc.project_security_g06.servicesinterfaces.IdCiudadService;

import java.util.List;

@Service
public class CiudadServiceImplement implements IdCiudadService {
    @Autowired
    private ICiudadRepository cS;

    @Override
    public void insert(Ciudad ciudad) {
        cS.save(ciudad);
    }

    @Override
    public List<Ciudad> list() {
        return List.of();
    }

    @Override
    public List<Ciudad> findByName(String nombre) {
        return List.of();
    }
}