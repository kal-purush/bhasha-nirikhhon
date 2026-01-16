/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package ec.edu.ups.estructura.arboles.contacto.controlador;

import ec.edu.ups.estructura.arboles.contacto.modelo.Contacto;
import ec.edu.ups.estructura.arboles.contacto.modelo.Node;
import ec.edu.ups.estructura.arboles.contacto.vista.Vista;

/**
 *
 * @author ACER
 */
public class ControladorContactos {

    private ArbolContactos arbolContactos;
    private Vista vista;

    public ControladorContactos(Vista vista) {
        this.vista = vista;
        this.arbolContactos = new ArbolContactos();
    }

    public void iniciar() {
        int opcionPrincipal = -1;
        while (opcionPrincipal != 0) {
            opcionPrincipal = vista.mostrarMenuPrincipal();

            // Instancia de [dato] segun la opcion escogida
            switch (opcionPrincipal) {
                case 1:
                    ingresarContacto();
                    break;
                case 2:
                    buscarContacto();
                    break;
                case 3:
                    eliminarContacto();
                    break;
                case 4:
                    agregarCorreo();
                    break;
                case 5:
                    agregarRedSocial();
                    break;
                case 6:
                    vista.mostrarMenuImprimir();
                    break;
                case 7:
                    
                    break;
                case 8:

                    break;
                case 0:
                    vista.mostrarMensaje("Saliendo del programa...");
                    break;
                default:
                    vista.mostrarMensaje("Opción inválida");
                    break;
            }
        }
    }

    public void ingresarContacto() {
        vista.mostrarMensaje("INGRESO DE UN CONTACTO");
        String nombre = vista.pedirNombre();
        String telefono = vista.pedirTelefono();
        arbolContactos.insert(new Contacto(nombre, telefono));
        vista.mostrarMensaje("Ingreso exitoso! :) ");
        Node root = arbolContactos.getRoot();
        arbolContactos.printTreeNode(root, "", true);
    }

    public void buscarContacto() {
        vista.mostrarMensaje("BUSQUEDA DE UN CONTACTO");
        String nombre = vista.pedirNombreBuscar();
        Contacto contacto = arbolContactos.buscarContacto(nombre);
        if (contacto != null) {
            vista.mostrarMensaje(contacto.toString());
        } else {
            vista.mostrarMensaje("El contacto con nombre: " + nombre + " no existe");
            vista.mostrarMensaje("¿Desea ingresar un nuevo contacto con el nombre: " + nombre + " ?");
            vista.mostrarMensaje("Ingrese una opcion: \n 1. Si \n 2.No");
            int opcionAgregar = vista.pedirOpcion();
            if (opcionAgregar == 1) {
                ingresarC(nombre);
            }
        }
    }

    public void ingresarC(String nombre) {
        vista.mostrarMensaje("INGRESO DE UN NUEVO CONTACTO");
        String telefono = vista.pedirTelefono();
        arbolContactos.insert(new Contacto(nombre, telefono));
        vista.mostrarMensaje("Ingreso exitoso! :) ");
    }

    public void eliminarContacto() {
        vista.mostrarMensaje("ELIMINACION DE UN CONTACTO");
        String nombre = vista.pedirNombreBuscar();
        Contacto contacto = arbolContactos.buscarContacto(nombre);
        if (contacto != null) {
            arbolContactos.eliminarContacto(nombre);
        } else {
            vista.mostrarMensaje("El contacto con nombre: " + nombre + " no existe, asi que no se elimino...");
        }
    }

    public void agregarCorreo() {
        vista.mostrarMensaje("INGRESO DE UN CORREO");
        String nombre = vista.pedirNombreBuscar();
        Contacto contacto = arbolContactos.buscarContacto(nombre);
        if (contacto != null) {
            String correo = vista.pedirCorreo();
            contacto.ingresarCorreo(correo);
            vista.mostrarMensaje("El correo se ha ingresado correctamente");

        } else {
            vista.mostrarMensaje("El contacto con nombre: " + nombre + " no existe");
        }
    }

    public void agregarRedSocial() {
        vista.mostrarMensaje("INGRESO DE UNA RED SOCIAL");
        String nombre = vista.pedirNombreBuscar();
        Contacto contacto = arbolContactos.buscarContacto(nombre);
        if (contacto != null) {
            String redSocial = vista.pedirRedSocial();
            String link = vista.pedirLink();
            contacto.ingresarRedSocial(redSocial, link);
            vista.mostrarMensaje("La redSocial se ha ingresado correctamente");

        } else {
            vista.mostrarMensaje("El contacto con nombre: " + nombre + " no existe");
        }
    }
    
    public void imprimirMenu() {
        int opcionImprimir = -1;
        while (opcionImprimir != 0) {
            opcionImprimir = vista.mostrarMenuImprimir();
            
            // Instancia de [dato] segun la opcion escogida
            switch (opcionImprimir) {
                case 1:
                    ////ingresarContacto();
                    System.out.println("CASO 1");
                    break;
                case 2:
                    System.out.println("CASO 1");
                    break;
                case 3:
                    System.out.println("CASO 1");
                    break;
                case 4:
                    System.out.println("CASO 1");
                    break;
                case 0:
                    vista.mostrarMensaje("Volviendo al menu principal...");
                    break;
                default:
                    vista.mostrarMensaje("Opción inválida");
                    break;
            }
        }
    }
    

}