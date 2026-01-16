package controladores;

import interfaces.ServicioCompradores;
import lombok.Getter;
import lombok.Setter;
import modelos.Compradores;
import modelos.Empleados;

import java.util.ArrayList;
import java.util.List;

public class AdministradorCompradores implements ServicioCompradores {

    @Setter @Getter
    private List<Compradores> compradoresRegistrados = new ArrayList<>();
    @Override
    public void agregarComprador(Compradores comprador) {
        //TODO: el producto debe settearse el ID
        this.compradoresRegistrados.add(comprador);
    }

    @Override
    public void modificarComprador(Compradores comprador) {
//TODO: logica para modificar empleado
        return;
    }

    @Override
    public void deshabilitarComprador(String compradorId) {
//TODO: logica para modificar empleado
        return;
    }
}