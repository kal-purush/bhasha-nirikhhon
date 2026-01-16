package com.plane.pojo.plane;

public class Worker {
    private int id;
    private final String firstName;
    private final String lastName;
    private final String position;

    public Worker(int id, String firstName, String lastName, String position) {
        this.id = id;
        this.firstName = firstName;
        this.lastName = lastName;
        this.position = position;
    }

    public Worker(String firstName, String lastName, String position) {
        this.firstName = firstName;
        this.lastName = lastName;
        this.position = position;
    }

//    private final String aeronauticalEngineer = "Mark Roseman";
//    private final String mechanicalEngineer = "Calvin Hibs";
//    private final String draftingEngineer = "Kevin Baker";
//    private final String operationsTechnician = "Alicia Tatcha";
//    private final String AircraftEquipmentMechanic = "Haryam Reyez";

}