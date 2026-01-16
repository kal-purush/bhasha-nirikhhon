public class problem16 {
    static class Vehicle {
        private int speed;
        private String manufacturer;
        private int yearOfManufacture;
        private String VIN;

        public Vehicle(int speed, String manufacturer, int yearOfManufacture, String VIN) {
            this.speed = speed;
            this.manufacturer = manufacturer;
            this.yearOfManufacture = yearOfManufacture;
            this.VIN = VIN;
        }

        public int getSpeed() {
            return speed;
        }

        public void setSpeed(int speed) {
            this.speed = speed;
        }

        public String getManufacturer() {
            return manufacturer;
        }

        public int getYearOfManufacture() {
            return yearOfManufacture;
        }

        public String getVIN() {
            return VIN;
        }

        public void move() throws VehicleException {
            if (speed == 0) {
                throw new VehicleException("Vehicle is not moving, there is a snag");
            }
            System.out.println("Vehicle is moving at speed: " + speed);
        }
    }

    static class VehicleException extends Exception {
        public VehicleException(String message) {
            super(message);
        }
    }


        public static void main(String[] args) {
            Vehicle motorcycle = new Vehicle(80, "Harley-Davidson", 2020, "MC12345");
            Vehicle car = new Vehicle(0, "Toyota", 2022, "CA67890");
            Vehicle truck = new Vehicle(50, "Ford", 2021, "TR24680");

            try {
                motorcycle.move();
                car.move();
                truck.move();
            } catch (VehicleException e) {
                System.out.println(e.getMessage());
            }
        }


}