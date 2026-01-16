package EV2.OOP.C_230203;

public class MainTime {
    public static void main(String[] args) {
        Time medianoche = new Time(0, 0);
        Time mediodia = new Time(12, 0, 0);
        Time tiempoPorDefecto = new Time();

        System.out.println("medianoche es " + medianoche);
        System.out.println("mediodía es " + mediodia);
        System.out.println("El valor de tiempoPorDefecto es " + tiempoPorDefecto);

        final int quince = 15;
        final int treinta = 30;
        final int noventa = 90;

        if (medianoche.addMinutes(quince)) {
            System.out.println("Después de ejecutar medianoche.addMinutes(quince), medianoche es "
                    + medianoche);
        }
        else {
            System.err.println("Error: medianoche.addMinutes(quince) ha devuelto false. medianoche"
                    + " es " + medianoche);
        }

        if (medianoche.addSeconds(treinta)) {
            System.out.println("Después de ejecutar medianoche.addSeconds(treinta), medianoche es "
                    + medianoche);
        }
        else {
            System.err.println("Error: medianoche.addSeconds(treinta) ha devuelto false. medianoche"
                    + " es " + medianoche);
        }

        if (medianoche.addSeconds(noventa)) {
            System.out.println("Después de ejecutar medianoche.addSeconds(noventa), medianoche es "
                    + medianoche);
        }
        else {
            System.err.println("Error: medianoche.addSeconds(noventa) ha devuelto false. medianoche"
                    + " es " + medianoche);
        }

        if (mediodia.addMinutes(noventa)) {
            System.out.println("Después de ejecutar mediodía.addMinutes(noventa), mediodía es "
                    + mediodia);
        }
        else {
            System.err.println("Error: mediodía.addMinutes(noventa) ha devuelto false. "
                    + "mediodía es " + mediodia);
        }

        if (mediodia.addHoras(quince)) {
            System.out.println("Después de ejecutar mediodía.addHoras(quince), mediodía es " + mediodia);
        }
        else {
            System.err.println("Error: mediodía.addHoras(quince) ha devuelto false. mediodía es "
                    + mediodia);
        }

        System.out.println("\nNuevo objeto \"tiempoPrueba\" creado con 23:59:59");

        Time tiempoPrueba = new Time(23, 59, 59);

        if (tiempoPrueba.addSeconds(0)) {
            System.out.println("Después de ejecutar tiempoPrueba.addSeconds(0), tiempoPrueba es "
                    + tiempoPrueba);
        }
        else {
            System.err.println("Error: tiempoPrueba.addSeconds(0) ha devuelto false. tiempoPrueba es "
                    + tiempoPrueba);
        }

        if (tiempoPrueba.addSeconds(300)) {
            System.out.println("Después de ejecutar tiempoPrueba.addSeconds(300), tiempoPrueba es "
                    + tiempoPrueba);
        }
        else {
            System.err.println("Error: tiempoPrueba.addSeconds(300) ha devuelto false. tiempoPrueba es "
                    + tiempoPrueba);
        }

        if (tiempoPrueba.addMinutes(1439)) {
            System.out.println("Después de ejecutar tiempoPrueba.addMinutes(1439), tiempoPrueba es "
                    + tiempoPrueba);
        }
        else {
            System.err.println("Error: tiempoPrueba.addMinutes(1439) ha devuelto false. tiempoPrueba es "
                    + tiempoPrueba);
        }

        if (tiempoPrueba.addHoras(25)){
            System.out.println("Después de ejecutar tiempoPrueba.addHoras(25), tiempoPrueba es "
                    + tiempoPrueba);
        }else {
            System.err.println("Error: tiempoPrueba.addHoras(25) ha devuelto false. tiempoPrueba es "
                    + tiempoPrueba);
        }
    }
}