package hk.edu.polyu.comp.comp2021.clevis.model;

import java.util.Objects;

public class Shape {
    private final String name;
    Shape next;
    Shape previous;

    public Shape(String name) {
        // DON'T FORGET VALIDATION
        // Should we consider that names of shapes should not be duplicated
        // or its actually stated idk my brain is empty
        this.name = name;

        if(cur!=null){
            cur.next=this;
            this.previous=cur;
        }
        cur = this;
    }

    public String getName() {
        return name;
    }

    public void getInfo(){}

    public static Shape cur=null;

    public static Shape findAShape(String name){        //finds and returns the shape with a name
        Shape temp = cur;
        while(!Objects.equals(temp.name, name)){
            temp=temp.previous;
        }
        return temp;
    }

    public static void delete(String name){     //delete function prototype part 1
        Shape target = findAShape(name);
        target.delete();
    }

    public void delete() {      //delete function prototype part 2
        if (this.previous != null) {
            this.previous.next = this.next;
        }
        if (this.next != null) {
            this.next.previous = this.previous;
        }
        if (this == cur) {
            cur = null;
        }
    }

    public static void ListTest(){      //Lists every single shape, from the newest to the oldest
        Shape temp = cur;
        while(temp != null){
            temp.getInfo();
            temp = temp.previous;
        }
    }
}


//===============[RECTANGLE CLASS]==========================================================================


class Rectangle extends Shape {
    public float x, y, w, h;

    Rectangle(String name, float x, float y, float w, float h) {
        super(name);
        this.x = x;
        this.y = y;
        this.w = w;
        this.h = h;
    }
    public void getInfo(){
        System.out.println(this.getName()+" x: "+ x + " y: "+y+" w: "+w+" h: "+h);
    }
}


//===============[LINE CLASS]======================================================================================


class Line extends Shape {
    public float x1, y1, x2, y2;

    Line(String name, float x1, float y1, float x2, float y2) {
        super(name);
        this.x1 = x1;
        this.x2 = x2;
        this.y1 = y1;
        this.y2 = y2;
    }
    public void getInfo(){
        System.out.println(this.getName()+" x1: "+ x1 + " y1: "+y1+" x2: "+x2+" y2: "+y2);
    }
}


//======================[CIRCLE CLASS]=============================================================================


class Circle extends Shape {
    public float x, y, r;

    Circle(String name, float x, float y, float r) {
        super(name);
        this.x = x;
        this.y = y;
        this.r = r;
    }
    public void getInfo(){
        System.out.println(this.getName()+" x: "+ x + " y: "+y+" r: "+r);
    }
}


//==================[SQUARE CLASS]=================================================================================


class Square extends Shape {
    public float x, y, l;

    Square(String name, float x, float y, float l) {
        super(name);
        this.x = x;
        this.y = y;
        this.l = l;
    }
    public void getInfo(){
        System.out.println(this.getName()+" x: "+ x + " y: "+y+" l: "+l);
    }
}

//==================[GROUP CLASS]=====================================================================

class Group extends Shape{
    public Shape s1, s2 ;
    Group(String name, Shape s1, Shape s2){
        super(name);
        this.s1=s1;
        this.s2=s2;
    }
    public void getInfo(){
        System.out.println("Group name: "+ this.getName());
        s1.getInfo();
        s2.getInfo();
    }
    public void delete(){
        s1.delete();
        s2.delete();
        s1=null;
        s2=null;
        if (this.previous != null) {
            this.previous.next = this.next;
        }
        if (this.next != null) {
            this.next.previous = this.previous;
        }
        if (this == cur) {
            cur = null;
        }
    }
}
