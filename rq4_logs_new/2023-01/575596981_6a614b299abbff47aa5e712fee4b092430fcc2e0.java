package EV2.OOP.C_230117;

import java.util.Scanner;

public class CuentaCorriente {
    static Scanner sc = new Scanner(System.in);

    // Variables de clase
    private final int numeroCuenta;
    private String nombre;
    private double saldo;

    // Constructor
    public CuentaCorriente(int numeroCuenta, String nombre){
        this.numeroCuenta = numeroCuenta;
        this.nombre = nombre;
        this.saldo = 0;
    }

    // Sobrecarga de constructores
    public CuentaCorriente(int numeroCuenta, String nombre, double saldo) {
        this.numeroCuenta = numeroCuenta;
        this.nombre = nombre;
        this.saldo = saldo;
    }

    // Getters y Setters
    public int getNumeroCuenta() {
        return numeroCuenta;
    }

    public String getNombre() {
        return nombre;
    }

    public double getSaldo() {
        return saldo;
    }

    public void setNombre(String nombre) {
        this.nombre = nombre;
    }

    public void setSaldo(double saldo) {
        this.saldo = saldo;
    }

    // Métodos
    public void ingresar(double cantidad){
        this.saldo += cantidad;
        System.out.println("Se han ingresado " + cantidad + "€ en la cuenta " + this.numeroCuenta + ".");
    }

    public boolean retirar(double cantidad){
        if (this.saldo >= cantidad){
            this.saldo -= cantidad;
            System.out.println("Se han retirado " + cantidad + "€ de la cuenta " + this.numeroCuenta + ".");
            return true;
        } else {
            System.out.println("No se puede retirar " + cantidad + "€ de la cuenta " + this.numeroCuenta + ".");
        }
        return false;
    }

    public void consultarSaldo(){
        System.out.println("Hay " +
                this.saldo + "€ en la cuenta " +
                this.numeroCuenta + " que pertenece a " +
                this.nombre + ".");
    }

    // Sobrecarga de métodos
    public void transferir(CuentaCorriente cuentaDestino, double cantidad){
        if(this.retirar(cantidad)){
            System.out.println("Se han transferido " +
                    cantidad + "€ de la cuenta " +
                    this.numeroCuenta + " a la cuenta " +
                    cuentaDestino.getNumeroCuenta() + ".");
            cuentaDestino.ingresar(cantidad);
        } else System.out.println("El ingreso en la cuenta " +
                cuentaDestino.getNumeroCuenta() +
                " no se ha podido realizar.");
    }

    public void transferir(CuentaCorriente cuentaDestino, double cantidad, double comision){
        if(this.retirar(cantidad + comision)){
            System.out.println("Se han transferido " +
                    (cantidad + comision) + "€ de la cuenta " +
                    this.numeroCuenta + " a la cuenta " +
                    cuentaDestino.getNumeroCuenta());
            cuentaDestino.ingresar(cantidad);
            System.out.println("Se ha cobrado una comisión de " + comision + "€.");
        } else System.out.println("El ingreso en la cuenta " +
                cuentaDestino.getNumeroCuenta() +
                " no se ha podido realizar.");
    }

    public static void main(String[] args) {
        CuentaCorriente cuenta1 = new CuentaCorriente(1, "Juan");
        CuentaCorriente cuenta2 = new CuentaCorriente(2, "Pepe", 1000);

        cuenta1.consultarSaldo();
        System.out.println("--------------------------------------------------------");
        cuenta1.setSaldo(100);
        cuenta1.consultarSaldo();
        System.out.println("--------------------------------------------------------");
        System.out.println("En la cuenta " + cuenta1.getNumeroCuenta() + " hay " + cuenta1.getSaldo() + "€.");
        System.out.println("--------------------------------------------------------");
        cuenta2.consultarSaldo();
        System.out.println("--------------------------------------------------------");
        System.out.print("¿Cuánto dinero quieres ingresar en la cuenta " + cuenta1.getNumeroCuenta() + "? ");
        cuenta1.ingresar(sc.nextDouble());
        cuenta1.consultarSaldo();
        System.out.println("--------------------------------------------------------");
        System.out.print("¿Cuánto dinero quieres retirar de la cuenta " + cuenta1.getNumeroCuenta() + "? ");
        cuenta1.retirar(sc.nextDouble());
        cuenta1.consultarSaldo();
        System.out.println("--------------------------------------------------------");
        System.out.print("¿Cuánto dinero quieres retirar de la cuenta " + cuenta2.getNumeroCuenta() + "? ");
        cuenta2.retirar(sc.nextDouble());
        cuenta2.consultarSaldo();
        System.out.println("--------------------------------------------------------");
        System.out.print("¿Cuánto dinero quieres transferir de la cuenta " + cuenta1.getNumeroCuenta() +
                " a la cuenta " + cuenta2.getNumeroCuenta() + "? ");
        cuenta1.transferir(cuenta2, sc.nextDouble());
        cuenta1.consultarSaldo();
        System.out.println("--------------------------------------------------------");
        cuenta2.consultarSaldo();
        System.out.println("--------------------------------------------------------");
        System.out.print("¿Cuánto dinero quieres transferir de la cuenta " + cuenta2.getNumeroCuenta() +
                " a la cuenta " + cuenta1.getNumeroCuenta() + "? ");
        cuenta2.transferir(cuenta1, sc.nextDouble(), 5.75);
        cuenta1.consultarSaldo();
        System.out.println("--------------------------------------------------------");
        cuenta2.consultarSaldo();
    }
}