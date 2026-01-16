package it.arduin.tables;

import java.util.ArrayList;

/**
 * Created by a on 16/03/2015.
 */
public class ColumnPair {
    String name,type;

    public ColumnPair(String name, String type) {
        if(name==null) name="";
        else if(name.isEmpty()) name="";
        if(type==null) name="";
        else if(type.isEmpty()) type="";
        this.name = name;
        this.type = type;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String toString(){
        return name+" : "+type;
    }

    static String[] getNameArray(ArrayList<ColumnPair> input){
        String[] nameArray=new String[input.size()];
        for(int i=0;i<input.size();i++) nameArray[i]=input.get(i).getName();
        return nameArray;
    }
    static String[] getTypeArray(ArrayList<ColumnPair> input){
        String[] typeArray=new String[input.size()];
        for(int i=0;i<input.size();i++) typeArray[i]=input.get(i).getType();
        return typeArray;
    }
    static String[] getStringDefinitionArray(ArrayList<ColumnPair> input){
        String[] array=new String[input.size()];
        for(int i=0;i<input.size();i++) array[i]=input.get(i).getName()+" , "+input.get(i).getType();
        return array;
    }
}