package controladores;

import interfaces.ServicioVentas;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import modelos.OrderList;
import modelos.Productos;
import modelos.Ventas;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Date;
import java.util.List;
import java.util.Scanner;
import java.util.regex.Pattern;

public class AdministradorVentaCompra implements ServicioVentas {

    @Getter @Setter
    private Double gananciasTotales;
    @Getter @Setter
    private Double gastosTotales;
    @Getter @Setter
    private List<Ventas> listaVentas;

    private List<Productos> listaProductos;
    private Ventas venta;
    private Scanner scanner;

    public String ejecutarVenta(List<Productos> listaProductos) throws InterruptedException {
        this.listaProductos = listaProductos;
        venta = new Ventas();

        System.out.println("Menu Principal/Crear venta");
        System.out.println("----------------------------------------");
        System.out.println("\n\t-> Creación de ventas\n");

        mostrarProductosDisponibles();

        agregarProductoAOrden();

        terminarVenta();

        Thread.sleep(1000);
        System.out.print(".");
        Thread.sleep(1000);
        System.out.print(".");
        Thread.sleep(500);

        return "menuPrincipal";
    }

    @Override
    public void mostrarProductosDisponibles() {
        int lengthNombre, lengthValor;
        System.out.println("\tProductos registrados");
        System.out.println("ID\t\t\t|Nombre\t\t\t\t\t\t|Valor unitario\t|Stock");
        for (Productos producto : this.listaProductos)
        {
            if(producto.getStatus() == "Inactivo") continue;
            System.out.print(producto.getProductoId());
            System.out.print("\t");
            System.out.print(producto.getNombre());

            lengthNombre = producto.getNombre().length();
            if(lengthNombre < 4)
            {
                System.out.print("\t\t\t\t\t\t\t");
            }else if(lengthNombre >= 4 && lengthNombre < 8)
            {
                System.out.print("\t\t\t\t\t\t");
            }else if(lengthNombre >= 8 && lengthNombre < 12)
            {
                System.out.print("\t\t\t\t\t");
            }else if(lengthNombre >= 12 && lengthNombre < 16)
            {
                System.out.print("\t\t\t\t");
            }
            else if(lengthNombre >= 16 && lengthNombre < 20)
            {
                System.out.print("\t\t\t");
            }else if(lengthNombre >= 20 && lengthNombre < 24)
            {
                System.out.print("\t\t");
            }else
            {
                System.out.print("\t");
            }

            System.out.print("$" + String.format("%.02f", producto.getValorUnitario()));

            lengthValor = String.valueOf(producto.getStock()).length();
            if(lengthValor < 3)
            {
                System.out.print("\t\t\t");
            }else if(lengthValor >= 3 && lengthValor < 7)
            {
                System.out.print("\t\t");
            }else
            {
                System.out.println("\t");
            }

            System.out.print(producto.getStock() + " unidad(es)\n");
        }
    }

    @Override
    public void agregarProductoAOrden() {

        System.out.println("\nEscribe el ID del producto que desees y haz enter para agregarlo");
        System.out.println("Cuando finalices, escribe Salir/salir");

        String productoId = "";
        String cantidad;

        Productos producto;

        do
        {
            productoId = getProductoId();
            if(productoId == "salir") break;

            cantidad = getCantidad(productoId);
            if(cantidad == "salir") break;

            String finalProductoId = productoId;

            producto = listaProductos.stream()
                    .filter(p -> p.getProductoId().equals(finalProductoId))
                    .findAny().orElse(null);

            venta.agregarProductoOrden(producto, Integer.parseInt(cantidad));

        }while (productoId.trim().toLowerCase() != "salir");

    }


    @Override
    public void terminarVenta() {

        this.scanner = new Scanner(System.in);

        String regex = "^[A-Za-z0-9+_.-]+@(.+)$";

        String mail;

        if(venta.getListaOrden().size() == 0) return;

        System.out.println("\t->Toma de productos finalizada");
        System.out.println("Digita el mail del cliente");
        do
        {

            mail = this.scanner.nextLine();

            if(mail.trim().length() == 0 || !Pattern.matches(regex, mail))
            {
                System.out.println("Debes ingresar un mail valido, intenta nuevamente");
                continue;
            }

            break;

        }while (true);

        crearComprobanteDePago();

        enviarMail();
    }

    public String getProductoId()
    {
        String productoId = "";
        Productos producto;
        this.scanner = new Scanner(System.in);

        do {

            System.out.print("\nID:");
            productoId = scanner.nextLine();
            if(productoId.trim().length() == 0)
            {
                System.out.print("\tPor favor, ingresa el ID del producto o digita Salir");
                continue;
            }

            if(productoId.trim().equalsIgnoreCase("salir")) return "salir";

            String finalProductoId = productoId;
            producto = listaProductos.stream()
                    .filter(p -> p.getProductoId().equals(finalProductoId))
                    .findAny().orElse(null);

            if(producto == null || producto.getStatus() == "Inactivo")
            {
                System.out.print("\tProducto invalido/inactivo, ingresa otro ID");
                continue;
            }

            return productoId;

        }while (true);
    }

    private String getCantidad(String productoId) {

        String cantidad = "";
        int cantidadInt;
        Productos producto;

        this.scanner = new Scanner(System.in);

        do {
            try
            {
                System.out.print("\nCantidad:");
                cantidad = scanner.nextLine();

                cantidad = cantidad.trim();

                if(cantidad.length() == 0)
                {
                    System.out.print("\n\tPor favor, ingresa una cantidad valida o digita Salir");
                    continue;
                }

                if(cantidad.trim().equalsIgnoreCase("salir")) return "salir";

                cantidadInt = Integer.parseInt(cantidad);

                if(cantidadInt < 0)
                {
                    System.out.print("\n\tPor favor, ingresa una cantidad valida o digita Salir");
                    continue;
                }

                producto = listaProductos.stream()
                        .filter(p -> p.getProductoId().equals(productoId))
                        .findAny().orElse(null);

                if(cantidadInt > producto.getStock())
                {
                    System.out.print("\n\tCantidad no disponible en stock, digita una nueva o Salir");
                    continue;
                }

                return cantidad;

            }catch (Exception ex)
            {
                System.out.print("\n\tPor favor, ingresa una cantidad valida o digita Salir");
            }

        }while (true);
    }

    private void crearComprobanteDePago()
    {
        String cuerpoComprobante = "\tTicket de compra";

        venta.setTicketDate(new Date());

        cuerpoComprobante += "\nFecha de compra: " + venta.getTicketDate().toString();

        cuerpoComprobante += "\n\nProductos seleccionados";

        cuerpoComprobante += "\nID\t\t\t|Cantidad\t\t|PrecioUni\t|Total\n";

        for (OrderList orden : this.venta.getListaOrden())
        {
            cuerpoComprobante += orden.toString() + "\n";
        }

        cuerpoComprobante += "---------------------------------\n";

        cuerpoComprobante += "\t\t\t\t\t\t\t\t\t$" + String.format("%.02f", this.venta.getTotal());

        cuerpoComprobante += "\nGRACIAS POR PREFERIRNOS";

        try {
            FileWriter ticket = new FileWriter("ticket.txt");
            ticket.write(cuerpoComprobante);
            ticket.close();
            System.out.println("Comprobante generado exitosamente");
        } catch (IOException e) {
            System.out.println("No fue posible crear comprobante");
            e.printStackTrace();
        }
    }

    private void enviarMail() {
        //logica para enviar mail
    }

}