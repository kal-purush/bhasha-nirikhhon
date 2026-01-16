
import java.util.ArrayList;

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

/**
 *
 * @author 2012100265
 */
public class AST {
    private int type;
    private String name;
    private double value;
    private final ArrayList <AST> children;
    
    public AST(int type){
        this.type = type;
        this.children = new ArrayList<>();
    }
    
    public AST(int type, String name){
        this(type);
        this.name = name;
    }
    
    public AST(int type, int value){
        this(type);
        this.value = value;
    }
    
    public AST(int type, double value){
        this(type);
        this.value = value;
    }
    
    public void add(AST n){
        children.add(n);
    }
    
    public AST getChild(int i){
        return children.get(i);
    }
    
    public double getValue(){
        return value;
    }
    
    public String getName(){
        return name;
    }
    
    public int getType(){
        return type;
    }
}