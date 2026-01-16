package com.example.group57moneyexchangeproject;

import java.io.*;
import java.util.ArrayList;
import java.util.List;



    public class Employee implements Serializable {
        private static final long serialVersionUID = 1L;

        private String type;
        private String employeeID;
        private String name;
        private String address;
        private String contactNumber;
        private String contract;

        public Employee(String type, String employeeID, String name, String address, String contactNumber, String contract) {
            this.type = type;
            this.employeeID = employeeID;
            this.name = name;
            this.address = address;
            this.contactNumber = contactNumber;
            this.contract = contract;
        }


        public String getType() {
            return type;
        }

        public String getEmployeeID() {
            return employeeID;
        }

        public String getName() {
            return name;
        }

        public String getAddress() {
            return address;
        }

        public String getContactNumber() {
            return contactNumber;
        }

        public String getContract() {
            return contract;
        }

        public static void saveToFile(List<Employee> employees, String filePath) {
            try (ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream(filePath))) {
                oos.writeObject(employees);
            } catch (IOException e) {
                System.err.println("Error saving employees to file: " + e.getMessage());
            }
        }

        public static List<Employee> loadFromFile(String filePath) {
            try (ObjectInputStream ois = new ObjectInputStream(new FileInputStream(filePath))) {
                return (List<Employee>) ois.readObject();
            } catch (IOException | ClassNotFoundException e) {
                System.err.println("Error loading employees from file: " + e.getMessage());
                return new ArrayList<>();
            }
        }
    }

