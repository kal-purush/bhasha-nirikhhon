package EV2.OOP.C_230125;

import java.util.Random;

public class CuentaBancaria {
    // Daniel Alonso Lázaro - 2023
    // Atributos
    private final short LONGITUD_IBAN = 22;
    private final String IBAN = "ES";
    private final String numeroCuenta;
    private final String titular;
    private final String moneda;
    private double saldo;
    private final boolean ventajasNomina;

    // Constructor
    public CuentaBancaria(String titular, double saldo, String moneda, boolean ventajasNomina) {
        this.numeroCuenta = generadorIBAN();
        this.titular = titular;
        this.saldo = saldo;
        this.moneda = moneda;
        this.ventajasNomina = ventajasNomina;
    }

    // Getters y Setters
    public String getNumeroCuenta() {
        return numeroCuenta;
    }

    public String getTitular() {
        return titular;
    }

    public double getSaldo() {
        return saldo;
    }

    public String getMoneda() {
        return moneda;
    }

    public boolean getVentajasNomina() {
        return ventajasNomina;
    }

    // Métodos
    public void depositarDinero(double cantidad) {
        saldo += cantidad;
        System.out.println("Se han ingresado "
                + normalizador(cantidad)
                + moneda
                + " en la cuenta "
                + numeroCuenta
                + ".\n");
    }

    public boolean retirarDinero(double cantidad) {
        if (esMultiplo(cantidad)) {
            System.out.println("No se pueden retirar "
                    + normalizador(cantidad)
                    + moneda
                    + " de la cuenta "
                    + numeroCuenta
                    + ".\nIntroduzca una cantidad múltiplo de 10.\n");
            return false;
        } else if (saldo >= cantidad) {
            saldo -= cantidad;
            System.out.println("Se han retirado "
                    + normalizador(cantidad)
                    + moneda
                    + " de la cuenta "
                    + numeroCuenta + ".\n");
            return true;
        } else {
            System.out.println("No se pueden retirar o transferir "
                    + normalizador(cantidad)
                    + moneda
                    + " de la cuenta "
                    + numeroCuenta
                    + ". Saldo insuficiente.\n");
            return false;
        }
    }

    public boolean retirarDinero(double cantidad, boolean transferencia) {
        if (saldo >= cantidad && transferencia) {
            saldo -= cantidad;
            return true;
        } else {
            return retirarDinero(cantidad);
        }
    }

    public void hacerTransferencia(CuentaBancaria cuentaDestino, double cantidad) {
        if (retirarDinero(cantidad, true)) {
            cuentaDestino.saldo += cantidad;
            System.out.println("Se han transferido "
                    + normalizador(cantidad)
                    + moneda
                    + " a la cuenta "
                    + cuentaDestino.numeroCuenta
                    + " desde la cuenta "
                    + numeroCuenta + ".\n");
        }
    }

    public void cobrarRecibo(double cantidad, String recibo){
        if (ventajasNomina) {
            saldo -= cantidad;
            System.out.println("Se ha cobrado el recibo de "
                    + recibo
                    + " con "
                    + normalizador(cantidad)
                    + moneda
                    + " de la cuenta "
                    + numeroCuenta
                    +"\n");
            if(saldo < 0) {
                System.out.println("La cuenta "
                        + numeroCuenta
                        + " ha quedado en números rojos.\n");
            }
        }else {
            System.out.println("Se ha devuelto el recibo "
                    + "\"" + recibo + "\""
                    + " con "
                    + normalizador(cantidad)
                    + moneda
                    + " de la cuenta "
                    + numeroCuenta
                    + ".\nLa cuenta no tiene ventajas de nómina y no admite saldos negativos, efectúe un ingreso.\n");
        }
    }

    public boolean esMultiplo(double cantidad) {
        double[] denominaciones = {50, 20, 10};
        for (double billete : denominaciones) {
            if (cantidad % billete == 0) {
                return false;
            }
        }
        return true;
    }

    public static String normalizador(double cantidad) {
        return cantidad % 1 == 0.0 ? Integer.toString((int) cantidad) : String.format("%.2f", cantidad);
    }

    public String generadorIBAN() {
        Random rand = new Random();
        StringBuilder sb = new StringBuilder();
        sb.append(IBAN);
        for(int i = 0; i < LONGITUD_IBAN; i++) {
            if (i == 2 || i == 7 || i == 12 || i == 17) {
                sb.append(" ");
            } else{
                sb.append(rand.nextInt(10));
            }
        }
        return String.valueOf(sb);
    }
}