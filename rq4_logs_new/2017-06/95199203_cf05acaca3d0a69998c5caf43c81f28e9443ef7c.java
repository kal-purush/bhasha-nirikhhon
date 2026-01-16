package ConstructorBehavior;
public class A {
    String x;
    int y;
    A(){
        this.x = "empty"; 
        this.y = 0;
        System.out.println("A: Empty constructor loaded.");
    }
    A(String x){
        this.x = x;
        System.out.println("A: Single(String) constructor loaded.");
    }
    A(int y){
        this.y = y;
        System.out.println("A: Single(int) constructor loaded.");
    }
    A(String x, int y){
        this.x = x; 
        this.y = y;
        System.out.println("A: Multiple constructor loaded.");
    }
    void setX(String x){this.x = x;}
    void setY(int y){this.y = y;}
    String getX(){return this.x;}
    int getY(){return this.y;}        
}