package com.example.spacetrader.entity;

public enum Resources {
    NOSPECIALRESOURCES("No Special Resources"), MINERALRICH("Mineral Rich"), MINERALPOOR("Mineral Poor"), DESERT("Desert"),
    LOTSOFWATER("Lots of Water"), RICHSOIL("Rich Soil"), POORSOIL("Poor Soil"), RICHFAUNA("Rich Fauna"), LIFELESS("Lifeless"),
    WEIRDMUSHROOMS("Weird Mushrooms"), LOTSOFHERBS("Lots of Herbs"), ARTISTIC("Artistic"), WARLIKE("Warlike");

    private final String name;
    Resources(String name){
        this.name = name;
    }

    @Override
    public String toString() {
        return name;
    }
}