package lectures.lecture1;

import java.util.Scanner;

public class Main {

    public static void main(String[] args) {
        Sensor h1 = new Sensor();
        TemperatureMeasure tc1 = new TemperatureMeasure(h1);
        for (int i = 0; i < 5; i++) {
            tc1.setValue();
        }
        ShowTemperature.Show(tc1);
        System.out.println();

        Sensor h2 = new Sensor();
        TemperatureMeasure tc2 = new TemperatureMeasure(h2);
        for (int i = 0; i < 5; i++) {
            tc2.setValue();
        }
        ShowTemperature.Show(tc2);
        System.out.println();

        System.out.println("Total: " + TemperatureMeasure.getTotal());
    }
    static class Sensor {
        private int value;

        public Sensor() {
            value = 0;
        }

        public void setValue(int value) {
            this.value += value;
        }

        public int getValue() {
            return value;
        }
    }
    static class MeasureCount {
        private int number;
        private static int total;

        public MeasureCount() {
            number = 0;
        }

        public void increment() {
            number++;
            total++;
        }

        public int getNumber() {
            return number;
        }

        public static int getTotal() {
            return total;
        }
    }
    class ITemperatureMeasure {
        public void setValue() {

        }

        public double getValue() {
            return 0;
        }
    }
    static class TemperatureMeasure {
        private Sensor h;
        private MeasureCount measure;

        public TemperatureMeasure(Sensor h) {
            measure = new MeasureCount();
            this.h = h;
        }

        public int getNumber() {
            return measure.getNumber();
        }

        public double getValue() {
            return (double) h.getValue() / measure.getNumber();
        }

        public void setValue() {
            int value;
            measure.increment();
            System.out.print("t[" + measure.getNumber() + "]= ");
            Scanner scanner = new Scanner(System.in);
            value = scanner.nextInt();
            h.setValue(value);
        }

        public static int getTotal() {
            return MeasureCount.getTotal();
        }
    }
    static class ShowTemperature {
        public static void Show(TemperatureMeasure t) {
            System.out.println(t.getNumber() + ": " + t.getValue() + " oC");
        }
    }
}