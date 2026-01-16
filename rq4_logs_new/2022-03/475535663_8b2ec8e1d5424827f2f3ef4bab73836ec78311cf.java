package eva2_4_heredity_practice;

/**
 * @author José Sebastian López Ibarra
 * Monday March 28 2022
 */

public class EVA2_4_HEREDITY_PRACTICE {

    /**
     * @param args the command line arguments
     */
    
    public static void main(String[] args) {
        Motorcycle motorcycle = new Motorcycle("Engine 998 CC", "Sports Driving", "Kawasaki", 2021, 
                "NinjaZX-10R", "DFI @ with 47mm Mikuni intake bodies with dual throttle \nvalves, two injectors per cylinder TCBI with electronic advance", 
                2, 17, 4, 1, 399990);
        motorcycle.printData();
        Car car = new Car("Hatchback", "Automatic", "Kia", 2021, "Rio", "121 HP @ 6,300 RPM", 4, 45, 4, 5, 300900);
        car.printData();
        Van van = new Van("4X4", "All terrain", "Jeep", 2022, "Wrangler", "3.6L Etorque", 4, 60, 6, 5,1109900);
        van.printData();
        Truck truck = new Truck("4X4", 4, "Kenworth", 2008, "T660", "CATERPILLAR C-15", 8, 248, 18, 3, 990800);
        truck.printData();
    }
    
}

class  Truck extends Vehicles {
    private String loadingCapacity;
    private int numAxes;
    
    public Truck() {
        super();
        System.out.println("TRUCK");
        loadingCapacity = "";
        numAxes = 0;
    }
    
    public Truck(String loadingCapacity, int numAxes, String mark, int year, String brand, String fuelType, int numTires,
                                int engineCapacity, int engineCylinders, int passengerCapacity, double price) {
        super(mark, year, brand, fuelType, numTires, engineCapacity, engineCylinders, passengerCapacity, price);
        System.out.println("TRUCK");
        this.loadingCapacity = loadingCapacity;
        this.numAxes = numAxes;
    }    
    
    @Override
    public void printData() {
        super.printData();
        System.out.println("Loading Capacity: " + loadingCapacity);
        System.out.println("Number of Axes: " + numAxes);
    }
}

class Van extends Vehicles {
    private String traction, bodywork;
    
    public Van() {
        super();
        System.out.println("VAN");
        traction = "";
        bodywork = "";
    }
    
    public Van(String traction, String bodywork, String mark, int year, String brand, String fuelType, int numTires,
                                int engineCapacity, int engineCylinders, int passengerCapacity, double price) {
        super(mark, year, brand, fuelType, numTires, engineCapacity, engineCylinders, passengerCapacity, price);
        System.out.println("VAN");
        this.traction = traction;
        this.bodywork = bodywork;
    }    
    
    @Override
    public void printData() {
        super.printData();
        System.out.println("Traction: " + traction);
        System.out.println("Bodywork: " + bodywork);
    }
}

class Car extends Vehicles {
    private String vehicleType, transmition;
    
    public Car() {
        super();
        System.out.println("CAR");
        vehicleType = "";
        transmition = "";
    }
    
    public Car(String vehicleType, String transmition, String mark, int year, String brand, String fuelType, int numTires,
                                int engineCapacity, int engineCylinders, int passengerCapacity, double price) {
        super(mark, year, brand, fuelType, numTires, engineCapacity, engineCylinders, passengerCapacity, price);
        System.out.println("CAR");
        this.vehicleType = vehicleType;
        this.transmition = transmition;
    }    
    
    @Override
    public void printData() {
        super.printData();
        System.out.println("Vehicle Type: " + vehicleType);
        System.out.println("Transmition Type: " + transmition);
    }
}

class Motorcycle extends Vehicles {
    private String motorType, drivingPosition;
    
    public Motorcycle() {
        super();
        System.out.println("MOTORCYCLE");
        motorType = "";
        drivingPosition = "";
    }
    
    public Motorcycle(String motorType, String drivingPosition, String mark, int year, String brand, String fuelType, int numTires,
                                int engineCapacity, int engineCylinders, int passengerCapacity, double price) {
        super(mark, year, brand, fuelType, numTires, engineCapacity, engineCylinders, passengerCapacity, price);
        System.out.println("MOTORCYCLE");
        this.motorType = motorType;
        this.drivingPosition = drivingPosition;
    }    
    
    @Override
    public void printData() {
        super.printData();
        System.out.println("Motorcycle Type: " + motorType);
        System.out.println("Driving Position: " + drivingPosition);
    }
    
}

class Vehicles {
    private String fuelType, mark, brand;
    private int numTires, engineCapacity, engineCylinders, passengerCapacity, year;
    private double price;
    
    public Vehicles() {
        System.out.println("    \nVEHICLES");
        mark = "";
        year = 0;
        brand = "";
        fuelType = "";
        numTires = 0;
        engineCapacity = 0;
        engineCylinders = 0;
        passengerCapacity = 0;
        price = 0;
    }
    
    public Vehicles(String mark, int year, String brand, String fuelType, int numTires, int engineCapacity, int engineCylinders, 
            int passengerCapacity, double price) {
        System.out.println("    \nVEHICLES");
        this.mark = mark;
        this.year = year;
        this.brand = brand;
        this.fuelType = fuelType;
        this.numTires = numTires;
        this.engineCapacity = engineCapacity;
        this.engineCylinders = engineCylinders;
        this.passengerCapacity = passengerCapacity;
        this.price = price;
    }

    Vehicles(String engine_998_CC, String sports_Driving, String kawasaki, int i, String ninjaZX10R, String dfi_with_47mm_Mikuni_intake_bodies_with_d, int i0, int i1, int i2, int i3, int i4, int i5) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    public void printData() {
        System.out.println("Mark: " + mark);
        System.out.println("Year: " + year);
        System.out.println("Brand: " + brand);
        System.out.println("Fuel Type: " + fuelType);
        System.out.println("Number of Tires: " + numTires);
        System.out.println("Engine Liter Capacity: " + engineCapacity);
        System.out.println("Engine Cylinders: " + engineCylinders);
        System.out.println("Capacity of Passengers: " + passengerCapacity);
        System.out.println("Price: $" + price);
    }
    
}