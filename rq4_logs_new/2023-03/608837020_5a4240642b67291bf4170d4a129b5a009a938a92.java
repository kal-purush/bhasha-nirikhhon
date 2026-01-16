package HomeWork5;

public class Air extends Transport {
    private double wingspan; // Размах крыльев (м)
    private int minRunwayLengths; // Минимальная длина взлетно-посадочной полосы для взлета

    public Air(String brand, int power, int maximumSpeed, int weight, double wingspan, int minRunwayLengths) {
        super(brand, power, maximumSpeed, weight);
        this.wingspan = wingspan;
        this.minRunwayLengths = minRunwayLengths;
    }

    @Override
    public void displayInfo() {
        super.displayInfo();
        System.out.printf("Размах крыльев: %.1f метров\t Минимальная длина взлетно-посадочной полосы: %d метров\n", wingspan, minRunwayLengths);
    }
}