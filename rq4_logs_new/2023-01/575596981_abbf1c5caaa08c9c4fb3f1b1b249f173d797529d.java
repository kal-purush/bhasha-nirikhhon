package ProgramasPruebas;

import javax.swing.*;
import java.awt.*;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Arrays;

public class vendingMachine {
    //    Daniel Alonso Lázaro - 2022
    /*
    Disponemos de una máquina automática de venta de bebidas. Disponemos de tres productos:
    agua cuyo precio es 0,50, refresco con precio de 0,75 y zumo cuyo precio es de 0,95 cts.
    El programa emitirá un menú que mostrará los productos y sus precios, así como la opción de salir.
    El programa pedirá la opción elegida y el dinero que se introduce para pagar.
    La forma de introducir el dinero para pagar en el programa es introducir el valor de cada moneda que ponemos
    en la máquina expresada en céntimos. El programa aceptará monedas hasta que se llegue al importe solicitado o se
    sobrepase la primera vez.
    Nuestra máquina acepta todas las monedas de euro a excepción de los 2 cts. y 1 cts.
    Al comienzo del día, la máquina dispone de 20 monedas de todas las cantidades necesarias para el cambio.
    Se debe dar el cambio correcto, con el menor número de monedas posibles. Quiero que aparezca en pantalla un
    mensaje de “INTRODUZCA IMPORTE EXACTO” si la máquina no dispone de monedas en 2 tipos de estas o más o si no
    dispone de monedas de un solo tipo y este es el de 5 cts., y que solo acepte este dinero.
    Al finalizar el programa nos debe dar el total del dinero disponible en la máquina, por unidad monetaria
    */
    public static void main(String[] args) {
        int[] monedas = {200, 100, 50, 20, 10, 5};
        int[] cantidad = {20, 20, 20, 20, 20, 20};
        int precio, cambio;
        String producto, salida = "salir";
        do {
            Object[] menu = menuOpciones();
            precio = (int)menu[1];
            if(!(producto = (String)menu[0]).equalsIgnoreCase(salida)) {
                if((cambio = (calculoPrecios(precio, cantidad, monedas) - precio)) < 0) break;
                expendedor(producto, cambio);
                if(cambio != 0) returnChange(cambio, monedas, cantidad);
            }
        }while(!producto.equalsIgnoreCase(salida));
        cierreMaquina(cantidad, monedas);
    }

    public static Object[] menuOpciones(){
        //Devuelve un array para fijar la elección del producto y el precio asociado al mismo.
        int precio = 0;
        ImageIcon icono = new ImageIcon(
                new ImageIcon("resources/vendingMachine/vending.png")
                        .getImage()
                        .getScaledInstance(150, 150, Image.SCALE_SMOOTH));
        String producto = (String) JOptionPane.showInputDialog(
                null,
                "Elegir un producto:",
                "GYLLENHAAL'S VENDING MACHINE",
                JOptionPane.INFORMATION_MESSAGE,
                icono,
                new Object[]{"Agua", "Refresco", "Zumo", "Cerveza", "Tonica", "Salir"},
                "Agua");
        switch (producto == null ? producto = "Salir" : producto) {
            case "Agua", "Tonica" -> printChoice(precio = 50, producto);
            case "Refresco" -> printChoice(precio = 75, producto);
            case "Zumo" -> printChoice(precio = 95, producto);
            case "Cerveza" -> printChoice(precio = 135, producto);
            case "Salir" -> JOptionPane.showMessageDialog(
                    null,
                    "VUELVA PRONTO\nLE ESPERAMOS",
                    "FINALIZANDO",
                    JOptionPane.INFORMATION_MESSAGE);
        }
        return new Object[]{producto, precio};
    }

    public static int imprimirPrecios(int precio){
        //Imprime el menu de precios y devuelve el precio del producto elegido.
        int cents = 0;
        ImageIcon icono = new ImageIcon(new ImageIcon("resources/vendingMachine/coins.png")
                .getImage()
                .getScaledInstance(150, 141, Image.SCALE_DEFAULT));
        StringBuilder mensajeMonedas = new StringBuilder("Introduce monedas:\n");
        String userChoice = ((String) JOptionPane.showInputDialog(
                null,
                mensajeMonedas.append("Precio: ")
                        .append(precio < 99 ? precio : String.format("%.2f", precio/100.0))
                        .append((precio > 99 ? precio == 100 ? " euro" : " euros" : " céntimos")),
                "PAGO",
                JOptionPane.INFORMATION_MESSAGE,
                icono,
                new Object[]{"2€", "1€", "50 cts", "20 cts", "10 cts", "5 cts"}, "2€"));
        switch (userChoice == null ? "" : userChoice) {
            case "2€" -> cents += 200;
            case "1€" -> cents += 100;
            case "50 cts" -> cents += 50;
            case "20 cts" -> cents += 20;
            case "10 cts" -> cents += 10;
            case "5 cts" -> cents += 5;
            default -> {
                return -1;
            }
        }
        return cents;
    }

    public static int calculoPrecios(int precio, int[] cantidad, int[] monedas) {
        //Calcula los precios y devuelve el cambio en céntimos.
        int cents, whileCents = 0, contadorCambio = 0;
        int[] contadorIntroducidas = new int[6];
        boolean contadorCambioBool = false, flag = true, ultimaMoneda = false;

        for(int i = 0; i < cantidad.length-2; i++) {
            if(cantidad[i] < 1) contadorCambio += 1;
            if(cantidad[i] == 1) ultimaMoneda = true;
        }

        if (cantidad[5] < 1 || contadorCambio >= 2 ||
                (contadorCambio == 1 && ultimaMoneda)) contadorCambioBool = true;

        do{
            if((cents = imprimirPrecios(precio)) == -1){
                return -1;
            }

            whileCents += cents;

            if(whileCents == precio) {
                //Precio justo
                flag = false;
                calderilla(cents, cantidad, monedas, contadorIntroducidas, true);
            }

            if(cents > precio && contadorCambioBool){
                //No hay para dar cambio
                JOptionPane.showMessageDialog(null, """
                                LA MÁQUINA NO DISPONE DE CAMBIO EN ESTOS MOMENTOS
                                ¡INTRODUZCA IMPORTE EXACTO!""",
                        "¡ATENCIÓN¡", JOptionPane.ERROR_MESSAGE);
                cents = 0;
            }

            if(cents > precio){
                //Necesita cambio
                flag = false;
                calderilla(cents, cantidad, monedas, contadorIntroducidas, true);
            }

            if(cents < precio) {
                //Hay que introducir más monedas
                contadorCambioBool = false;
                if((precio - whileCents) / 100.0 > 0) {
                    //Controla que el importe introducido sea distinto de 0
                    calderilla(cents, cantidad, monedas, contadorIntroducidas, true);
                    cents = insufficientCoins(true, precio, whileCents);
                }

                if(whileCents > precio){
                    contadorCambioBool = true;
                    calderilla(cents, cantidad, monedas, contadorIntroducidas, false);
                    whileCents = insufficientCoins(false, precio, whileCents);
                    jDialog("La devolución se ha completado con éxito\nIntroduzca el precio de nuevo.",
                            "INFORMACIÓN", 0, 350, 100,
                            "resources/vendingMachine/infoGreen.png");
                }
            }
        }while(flag);
        return whileCents;
    }

    public static void calderilla(int calderilla, int[] cantidad, int[] monedas,
                                  int[] contadorIntroducidas, boolean sumar){
        //Hace el recuento de monedas y las redistribuye en el array de cantidad
        //tanto si es para sumarlas como si es para restarlas en caso de error.
        if(sumar){
            for (int i = 0; i < cantidad.length; i++) {
                if (calderilla == monedas[i]) {
                    cantidad[i]++;
                    contadorIntroducidas[i]++;
                }
            }
            Arrays.fill(contadorIntroducidas, 0);
        }else{
            for(int i = 0; i < cantidad.length; i++) {
                for(int j = 0; j < contadorIntroducidas[i]; j++) cantidad[i]--;
            }
        }
    }

    public static void printChoice(int precio, String producto) {
        //Imprime los productos disponibles y el precio.
        jDialog(String.format("\n\tEl precio de"+
                        ((producto.equalsIgnoreCase("Cerveza") ||
                                producto.equalsIgnoreCase("Tonica")) ? " la " : "l ")+
                        "%s es %.2f€\n\n", producto.toLowerCase(), precio/100.0),
                "INFORMACIÓN",
                0, 380, 100, "resources/vendingMachine/infoGreen.png");
    }

    public static void jDialog(String format, String titulo, int tiempo, int width, int height, String... icon) {
        //Crea un JDialog con un mensaje y un icono.
        ImageIcon icono = null;
        if (icon != null) {
            icono = new ImageIcon(
                    new ImageIcon(icon[0])
                            .getImage()
                            .getScaledInstance(32 , 32, Image.SCALE_SMOOTH));
        }
        JDialog dialog = new JDialog();
        if (icon!=null) dialog.setIconImage(icono.getImage());
        dialog.setTitle(titulo);
        dialog.setModalityType(Dialog.DEFAULT_MODALITY_TYPE);
        dialog.setContentPane(new JOptionPane(
                format,
                JOptionPane.INFORMATION_MESSAGE,
                JOptionPane.DEFAULT_OPTION,
                null,
                new Object[]{},
                null));
        dialog.setDefaultCloseOperation(JDialog.DO_NOTHING_ON_CLOSE);
        dialog.pack();
        dialog.setSize(width, height);
        dialog.setLocationRelativeTo(null);
        retardoJOptionPane(3 + tiempo);
        dialog.setVisible(true);
    }

    public static void returnChange(int cambio, int[] monedas, int[] cantidad){
        //Devuelve el cambio.
        double euros;
        StringBuilder acumulador = new StringBuilder();
        for(int i = 0; i < monedas.length; i++){
            euros = monedas[i]/100.0;
            if(monedas[i] > 99){
                if(cambio >= monedas[i]){
                    acumulador.append(String.format("Monedas de "
                            +(euros%1==0.0
                            ? (int)euros
                            : String.format("%.2f", euros))
                            +" euro: "+cambio/monedas[i]+"\n"));
                    cambio%=monedas[i];
                    cantidad[i]-=1;
                }
            }else if (cambio >= monedas[i]){
                acumulador.append(String.format("Monedas de %d céntimos: %d\n", monedas[i], (cambio/monedas[i])));
                cambio%=monedas[i];
                cantidad[i]-=1;
            }
        }
        jDialog(String.valueOf(acumulador), "CAMBIO", 0, 300, 150,
                "resources/vendingMachine/infoGreen.png");
    }

    public static void cierreMaquina(int[] cantidad, int[] monedas){
        //Formatea e imprime los datos económicos de la máquina.
        double[] beneficios = new double[6];
        double euros, total = 0;
        StringBuilder acumulador = new StringBuilder("La máquina tiene:\n");
        for(int i = 0; i < beneficios.length; i++) {
            euros = monedas[i]/100.0;
            beneficios[i] = new BigDecimal(cantidad[i]*euros)
                    .setScale(2, RoundingMode.HALF_UP)
                    .doubleValue();
            acumulador.append(String.format((beneficios[i]%1==0.0
                    ? (int)beneficios[i] : String.format("%.2f", beneficios[i]))
                    +(beneficios[i] == 1 ? " euro en " : " euros en ")
                    +(cantidad[i] == 1 ? cantidad[i]+" moneda de " : cantidad[i]+" monedas de ")
                    +(euros < 1 ? (int)(euros*100)+" céntimos\n" : (euros%1==0.0
                    ? (int)euros : Double.toString(euros))+((monedas[i] == 200)
                    ? " euros\n" : " euro\n"))));
            total += beneficios[i];
        }
        JOptionPane.showMessageDialog(
                null,
                acumulador,
                "DESGLOSE DE VENTAS",
                JOptionPane.INFORMATION_MESSAGE);
        JOptionPane.showMessageDialog(
                null,
                String.format("Los beneficios totales del día han sido:\n%.2f€", total),
                "BENEFICIOS DEL DÍA",
                JOptionPane.INFORMATION_MESSAGE);
    }

    public static int insufficientCoins(boolean insuficiente, int precio, int whileCents) {
        //Devuelve el mensaje de dinero insuficiente y pone los céntimos a 0.
        if(insuficiente){
            jDialog(String.format("""
                                    POR FAVOR, CONTINUE INTRODUCIENDO MONEDAS
                                    Faltan %.2f euros por introducir
                                    """,
                            Math.abs(((precio - whileCents) / 100.0))),
                    "DINERO INSUFICIENTE", 0, 400, 150,
                    "resources/vendingMachine/warningRed.png");
            return 0;
        }else{
            jDialog(String.format("""
                                    Desafortunadamente, la máquina no dispone de cambio.
                                    No se pueden devolver %.2f euros.
                                    Iniciando proceso de devolución.
                                    
                                    Espere...""",
                            Math.abs(((precio - whileCents) / 100.0))),
                    "PROCEDIENDO A LA DEVOLUCIÓN",
                    2, 430, 160, "resources/vendingMachine/infoGreen.png");
        }
        return 0;
    }

    public static void expendedor(String producto, int cambio){
        //Imprime el mensaje de compra realizada con éxito.
        if (cambio > 0){
            jDialog(String.format("¡Expendiendo producto y cambio!\n\nProducto: %s\nCambio: %.2f€\n",
                            producto.toUpperCase(), cambio/100.0), "EXPENDIENDO", 0, 300, 150,
                    "resources/vendingMachine/infoGreen.png");
        }
    }

    public static void retardoJOptionPane(double tiempoEnSecs){
        //Retardo para el JDialog.
        Thread temporizador = new Thread(() -> {
            try {
                Thread.sleep((long) (tiempoEnSecs*1000));
            } catch(InterruptedException e) {
                e.printStackTrace();
            }
            JOptionPane.getRootFrame().dispose();
        });
        temporizador.start();
    }
}