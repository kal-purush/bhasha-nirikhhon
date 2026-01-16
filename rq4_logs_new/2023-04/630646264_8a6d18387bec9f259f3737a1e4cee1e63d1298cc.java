package Controlador;

import Model.Cabagna;
import Model.Carpa;
import Model.Hotel;
import Model.MedioDeAlojamiento;
import javax.rmi.CORBA.Tie;
import java.util.ArrayList;


public class EmpresaTurismo {

    //Atributos
    private ArrayList<MedioDeAlojamiento> empresa;

    public EmpresaTurismo() {
    }

    public EmpresaTurismo(ArrayList<MedioDeAlojamiento> empresa) {
        this.empresa = empresa;
    }

    public ArrayList<MedioDeAlojamiento> getEmpresa() {
        return empresa;
    }

    public void setEmpresa(ArrayList<MedioDeAlojamiento> empresa) {
        this.empresa = empresa;
    }

    public int buscarAlojamiento(int dniCliente) {

        for (int i = 0; i < empresa.size(); i++) {

            //definir una condicion para encontrar el alojamiento
            if (empresa.get(i).getDatosCliente().getDni() == 0) {

                //encontro el alojamiento en la coleccion
                return i;


            }

        }

        return -1; //retorna -1 si no encontro el objeto en la coleccion

    }

    public void ingresarClientesHotel(Hotel hotel) {
        if (buscarAlojamiento(hotel.getDatosCliente().getDni()) == -1) {
            empresa.add(hotel);
        }

    }

    public void ingresarClientesCabagna(Cabagna cabagna) {
        if (buscarAlojamiento(cabagna.getDatosCliente().getDni()) == -1) {
            empresa.add(cabagna);

        }
    }

    public void ingresarClientesCarpa(Carpa carpa) {
        if (buscarAlojamiento(carpa.getDatosCliente().getDni()) == -1) {
            empresa.add(carpa);

        }


    }


    //****************2.	Mostrar medios de alojamiento **************************************

    public String mostrarAlojamientos() {
        String mensaje = "";
        Hotel hotel = null;
        Cabagna cabagna = null;
        Carpa carpa = null;
        int contador = 0;
        for (int i = 0; i < empresa.size(); i++) { //iterar en la colección
            if (empresa.get(i) instanceof Hotel) { //el alojamiento es un hotel
                hotel = (Hotel) empresa.get(i);

                mensaje = mensaje + "\n se encuentran registrados los siguientes clientes: " + hotel.getDatosCliente();
                contador++;
            }
            if (empresa.get(i) instanceof Cabagna) { //el alojamiento es una cabagna
                cabagna = (Cabagna) empresa.get(i);

                mensaje = mensaje + "\n se encuentran registrados los siguientes clientes: " + cabagna.getDatosCliente();
                contador++;
            }
            if (empresa.get(i) instanceof Carpa) { //el alojamiento es una carpa
                carpa = (Carpa) empresa.get(i);

                mensaje = mensaje + "\n se encuentran registrados los siguientes clientes: " + carpa.getDatosCliente();
                contador++;
            }

        }

        if (contador == 0) {
            mensaje = "no hay registrados clientes en el Medio de Alojamiento seleccionado :/";
        }
        return mensaje;
    }

    //3.	Valor Venta
    //En Valor Venta mostrará el valor Venta y número del calzado de uno en particular.
    public String datosDelCliente(int dni) {
        String mensaje = "";
        for (int i = 0; i < empresa.size(); i++) { //iterar en la colección
            if (empresa.get(i).getDatosCliente().getDni() == 0) {
                //lo encontre
                return "El cliente: " + empresa.get(i).getDatosCliente().getDni() + " se encuentra alojado en: " +
                        empresa.get(i).getDatosCliente();
            }
        }

        return "El cliente " + dni + " no se encuentra registrado en ningun medio de alojamiento :/";
    }
    //****************4.	Total Adicional  **************************************

    public int totalAdicional() {
        int total = 0;
        Cabagna cabagna = null;
        Hotel hotel = null;

        for (int i = 0; i < empresa.size(); i++) { //iterar en la colección
            if (empresa.get(i) instanceof Cabagna) {
                cabagna = (Cabagna) empresa.get(i);
                total = total + cabagna.valorAdicional();

            } else if (empresa.get(i) instanceof Hotel) {
                hotel = (Hotel) empresa.get(i);
                total = total + hotel.valorAdicional();

            }

        }
        return total;
    }

    //****************5.	Total bono descuento **************************************
    public int totalDescuentos() {
        int total = 0;
        Hotel hotel = null;
        Cabagna cabagna = null;
        Carpa carpa = null;

        for (int i = 0; i < empresa.size(); i++) { //iterar en la colección
            if (empresa.get(i) instanceof Hotel) {
                hotel = (Hotel) empresa.get(i);
                total = total + (hotel.bonoDescuento() * hotel.getCantNoches());

            } else if (empresa.get(i) instanceof Cabagna) {
                cabagna = (Cabagna) empresa.get(i);
                total = total + (cabagna.bonoDescuento() * cabagna.getCantNoches());

            } else if (empresa.get(i) instanceof Carpa) {
                carpa = (Carpa) empresa.get(i);
                total = total + (carpa.bonoDescuento() * carpa.getCantNoches());
            }
        }
        return total;
    }

    public String cantidadMediosDeAlojamiento() {
        String mensaje = "";

        for (int i = 0; i < empresa.size(); i++) {
            if (i > 0) {
                mensaje = mensaje + "La cantidad de medios de alojamiento en uso es: " + empresa.size();
                break;
            }
        }
        return mensaje;
    }

    public String valorACancelar(int dni) {
        int total = 0;
        for (int i = 0; i < empresa.size(); i++) {
            if (empresa.get(i).getDatosCliente().getDni() == 0) {
                return "El valor a cancelar por el cliente: " + empresa.get(i).getDatosCliente().getDni() + "es: $" + empresa.get(i).valorACancelar();
            }
        }
        return "El cliente no pudo ser identificado";
    }

    public String incrementoValorBase(int dni) {
        Cabagna cabagna = null;
        for (int i = 0; i < empresa.size(); i++) {
            if (empresa.get(i).getDatosCliente().getDni() == 0) {
                if (empresa.get(i) instanceof Cabagna) {
                    cabagna = (Cabagna) empresa.get(i);
                    return "El incremento al valor base es de: $" + cabagna.incrementaValorBase();

                }
            }
        }
                return "No corresponde un incremento del valor base";

        }


    }