/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.eci.cvds.view;

import com.google.inject.Inject;
import edu.eci.cvds.samples.entities.Cliente;
import edu.eci.cvds.samples.services.ServiciosAlquiler;
import edu.eci.cvds.samples.services.ServiciosAlquilerFactory;
import java.util.List;
import javax.faces.bean.*;

/**
 *
 * @author 2153043
 */
@ManagedBean(name = "videoRental")
@ApplicationScoped
public class BeanServiciosAlquiler extends BasePageBean {
   
    private ServiciosAlquiler servicioAlquiler;
    private Cliente selectedCliente;
    public BeanServiciosAlquiler() {
        servicioAlquiler = ServiciosAlquilerFactory.getServiciosAlquiler();
    }

    public List<Cliente> getTodosLosClientes() {
        List<Cliente> clientes= null;
        try {
            clientes=servicioAlquiler.consultarClientes();
        } catch (Exception e) {

        }
        return clientes;

    }
    public Cliente consultarCliente(long docu){
        Cliente cliente= null;
        try{
        cliente= servicioAlquiler.consultarCliente(docu);
        }catch(Exception e){}
        return cliente;
    }
    public Cliente getSelectedCliente(){
        return selectedCliente;
    }
}