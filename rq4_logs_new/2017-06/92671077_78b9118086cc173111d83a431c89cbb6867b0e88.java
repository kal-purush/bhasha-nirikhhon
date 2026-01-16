package core;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Scanner;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.swing.JOptionPane;

public class CarInventorySystem
{
    /*
    public String year;
    public String make;
    public String model;
    public String carClass;
    public String ID;
    */
    
    private InventorySystem inventory;
    
    public CarInventorySystem(InventorySystem inv) {
        this.inventory = inv;
    }

    public Vehicle AddCar(int ID, String brand, String model, String color, int year, 
            String carClass, boolean availability, double dailyPrice) {
        Vehicle newVehicle = new Vehicle();
        newVehicle.setID(ID);
        newVehicle.setMake(brand);
        newVehicle.setModel(model);
        newVehicle.setColor(color);
        newVehicle.setYear(year);
        newVehicle.setCarClass(carClass);
        newVehicle.setAvailability(availability);
        newVehicle.setDailyPrice(dailyPrice);
        
        StoreVehicle(newVehicle);
        
        return newVehicle;
    }
    
    public void UpdateCar(String newBrand, String newModel, String newColor, 
            int newYear, String newCarClass, boolean newAvailability, 
            double newDailyPrice) {
        
    }
    
    public Vehicle SearchVehicle(int searchID) {
        ArrayList<ArrayList> vehicles = new ArrayList<>();
        ArrayList<Object> vehicleInformation = null;
        Vehicle foundVehicle = null;
        String vehicleEntry;
        try {
            Scanner vehicleScan = new Scanner(new File("CarBase.txt"));
            while(vehicleScan.hasNextLine()) {
                vehicleEntry = vehicleScan.nextLine();
//                System.out.println(vehicleEntry);
                vehicleInformation = CreateVehicleEntry(vehicleEntry);
                vehicles.add(vehicleInformation);
//                if(VerifyFoundVehicle(searchID, vehicleInformation)) {
//                    System.out.println("Vehicle found!");
//                    JOptionPane.showMessageDialog(null, "Found vehicle: " + 
//                            vehicleEntry, "Vehicle Found", 
//                            JOptionPane.INFORMATION_MESSAGE);
//                }
//                else {
//                    System.out.println("Vehicle not found!");
//                    JOptionPane.showMessageDialog(null, 
//                            "Sorry, the vehicle was not found. Please try again.", 
//                            "Vehicle Not Found", JOptionPane.INFORMATION_MESSAGE);
//                }
            }
            vehicleScan.close();
            
            //All of below works for sure, but need to clean up
            if(VerifyFoundVehicle(searchID, vehicles)) {
                System.out.println("Vehicle found!");
                
                ArrayList<Object> information = GetFoundInformation(searchID, vehicles);

                foundVehicle = InitializeVehicleInformation(information);
                
//                foundVehicle = new Vehicle();
//                foundVehicle.setMake(vehicleInformation.get(1).toString());
//                foundVehicle.setModel(vehicleInformation.get(2).toString());
//                foundVehicle.setColor(vehicleInformation.get(3).toString());
//                foundVehicle.setYear(Integer.parseInt(vehicleInformation.get(4).toString()));
//                foundVehicle.setCarClass(vehicleInformation.get(5).toString());
//                foundVehicle.setDailyPrice(Double.parseDouble(vehicleInformation.get(7).toString()));
                
//                    JOptionPane.showMessageDialog(null, "Found vehicle: " + 
//                            vehicleEntry, "Vehicle Found", 
//                            JOptionPane.INFORMATION_MESSAGE);
            }
        } catch (FileNotFoundException ex) {
            Logger.getLogger(CarInventorySystem.class.getName()).log(Level.SEVERE, null, ex);
        }
        
        return foundVehicle;
    }
    
    public ArrayList CreateVehicleEntry(String vehicleInformation) {
        ArrayList<Object> vehicleEntries = new ArrayList<>();
        String[] newVehicleEntry = vehicleInformation.split(" ");
        for(int i=0;i<newVehicleEntry.length;i++) {
            vehicleEntries.add(newVehicleEntry[i]);
        }
        
//        //Debugging
//        for(Object e:vehicleEntries) {
//            System.out.println(e.toString());
//        }
        
        return vehicleEntries;
    }
    
    public boolean VerifyFoundVehicle(int verifyID, ArrayList<ArrayList> vehicles) {
        for(int i=0;i<vehicles.size();i++) {
            for(int j=0;j<vehicles.get(i).size();j++) {
                if(verifyID == Integer.parseInt(vehicles.get(i).get(0).toString())) {
                    return true;
                }
            }
        }
//        if(verifyID == Integer.parseInt(vehicle.get(0).toString())) {
//            return true;
//        }
        return false;
    }
    
    public ArrayList<Object> GetFoundInformation(int id, ArrayList<ArrayList> vehicles) {
        ArrayList<Object> entry = new ArrayList<>();
        for(ArrayList vehicle:vehicles) {
            for(int j=0;j<vehicle.size();j++) {
                if(id == Integer.parseInt(vehicle.get(0).toString())) {
                    entry = vehicle;
                }
            }
        }
        return entry;
    }
    
    public Vehicle InitializeVehicleInformation(ArrayList vehicleInformation) {
        Vehicle foundVehicle = new Vehicle();
        
        foundVehicle.setMake(vehicleInformation.get(1).toString());
        foundVehicle.setModel(vehicleInformation.get(2).toString());
        foundVehicle.setColor(vehicleInformation.get(3).toString());
        foundVehicle.setYear(Integer.parseInt(vehicleInformation.get(4).toString()));
        foundVehicle.setCarClass(vehicleInformation.get(5).toString());
        foundVehicle.setAvailability(Boolean.valueOf(vehicleInformation.get(6).toString()));
        foundVehicle.setDailyPrice(Double.parseDouble(vehicleInformation.get(7).toString()));
        
        return foundVehicle;
    }
    
    public void StoreVehicle(Vehicle newVehicle) {
        try {
            FileWriter vehicleEntry = new FileWriter("CarBase.txt", true);
            vehicleEntry.write(newVehicle.getID() + " " + newVehicle.getMake() + 
                    " " + newVehicle.getModel() + " " + newVehicle.getColor() + 
                    " " + newVehicle.getYear() + " " + newVehicle.getCarClass() + 
                    " " + newVehicle.isAvailability() + " " + 
                    newVehicle.getDailyPrice());
            vehicleEntry.write(System.getProperty("line.separator"));
            inventory.ReadVehicleEntry("CarBase.txt");
            vehicleEntry.close();
        }
       catch(Exception ex) {
            Logger.getLogger(Customer.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
}