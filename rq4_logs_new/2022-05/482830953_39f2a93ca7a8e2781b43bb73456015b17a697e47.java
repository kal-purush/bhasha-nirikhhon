package Main;

import Service.*;
import Vehicle.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class Main {

    public static List<Service> serviceList = new ArrayList<>();
    //public static Vehicle passengerCar = new PassengerCar();

    public static void main(String[] args) throws IOException {

        Service<Vehicle> tempAllService;

       // serviceList = CreateServiceList.fillServiceList(GetParameter.getAmountService());
        System.out.println();
        //Print Service
      //  PrintList.printServiceList(serviceList);
        System.out.println("Create passenger car:");
        PassengerCar passengerCar = new PassengerCar.Builder()
                .withHealthStatus(HealthStatus.UNKNOWN)
                .withServiceStatus(ServiceStatus.UNKNOWN)
                .withModel(GetParameter.getVehicleModel())
                .withMaxPassengers(GetParameter.getMaxPassengers())
                .build();
        System.out.println("Create truck:");
        Truck truck = new Truck.Builder()
                .withHealthStatus(HealthStatus.UNKNOWN)
                .withServiceStatus(ServiceStatus.UNKNOWN)
                .withModel(GetParameter.getVehicleModel())
                .withMaxLoad(GetParameter.getMaxLoad())
                .build();

        System.out.println(passengerCar.toString());
        System.out.println(truck.toString());
// services creation
        System.out.println("Create service for truck:");
        Service<Truck> truckService = new ServiceForTruck.Builder()
                .withServiceName(GetParameter.getServiceName())
                .build();
        System.out.println("Create service for passenger car:");
        Service<PassengerCar> passengerCarService = new ServiceForPassengerCar.Builder()
                .withServiceName(GetParameter.getServiceName())
                .build();
        System.out.println("Create service for all:");
        Service<Vehicle> serviceForAll = new ServiceForAll.Builder()
                .withServiceName(GetParameter.getServiceName())
                .build();


        System.out.println("Repair log:");
        truckService.repair(truck);
        passengerCarService.repair(passengerCar);
        serviceForAll.repair(truck);
        serviceForAll.repair(passengerCar);

  }
}