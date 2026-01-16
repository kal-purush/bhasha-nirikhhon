package net.codetojoy;

public class Donut {
    public enum Type { TRADITIONAL, JELLY, OTHER };
        
    private final int price;
    private final int calories;
    private final String name;
    private final Type type;

    public int getPrice() { return price; }
    public int getCalories() { return calories; }
    public String getName() { return name; }
    public Type getType() { return type; }
    
}