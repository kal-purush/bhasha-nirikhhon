import java.util.Scanner;

public class ventajuegos {
    public static void main(String[] args) {
        Scanner scanner = new Scanner(System.in);
        double montoTotal, subTotal, igv, PORCENTAJE_IGV = 0.18;
        int cantidad, opciones;
        double precio;
        String cheekyMoneky;
        // Definición de productos y precios
        String[] productos = {
                "Cheeky Monkey Ticks", "Fancy Racing", "DuoDrive", "Club 3.04", "Frog Frenzi II", "Carousel",
                "Piratas Hook Ticks", "SkyLanders Cloud Patrol Ticks", "Big Teeth Monkey Ticks", "Lane Máster Ticks",
                "Crazy Hoop Ticks", "Deal or No Deal Ticks", "Dino Pop Ticks", "Funny Frog Ticks",
                "Jungle Drummer - Mantenimiento Ticks", "Coconut Bash Ticks", "2 Motogp", "Batman Ticks",
                "2 Ghost Squad Evolution", "2 After Dark", "Funky Gatos Ticks", "Congo Bongo Ticks", "Carros Chocantes"
        };

        double[] precios = {
                3.20, 3.80, 3.20, 3.04, 3.30, 3.80,
                3.70, 3.20, 4.20, 3.30, 3.20, 3.20,
                3.20, 4.20, 3.20, 3.20, 3.20, 3.20,
                3.20, 4.20, 3.20, 2.70, 7
        };
    }
}