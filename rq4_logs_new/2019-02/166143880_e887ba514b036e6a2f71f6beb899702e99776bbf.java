package com.example.spacetrader.entity;

public class SolarSystem {
    private Point location;
    private String name;
    private TechLevel techLevel;
    private Planet planet;

    public SolarSystem(Point location, String name, TechLevel techLevel) {
        this.location = location;
        this.name = name;
        planet = new Planet(name);
        this.techLevel = techLevel;
    }

    public String getName() {
        return name;
    }

    public Planet getPlanet() { return planet; }

    public TechLevel getTechLevel() { return techLevel; }

    public Point getLocation() {
        return location;
    }


    @Override
    public String toString() {
        return ("{Solar System: " + this.name + " | Location: (" + location.getX() + ", " + location.getY() +  ") | TechLevel: " + techLevel.toString() + " | Planets: [" + planet.toString() + "] }");
    }
}