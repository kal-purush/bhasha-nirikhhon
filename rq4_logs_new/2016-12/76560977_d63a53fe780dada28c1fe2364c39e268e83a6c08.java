package Editor;

import Editor.Xtypes.Xattr;
import Editor.Xtypes.Xrule;
import Editor.Xtypes.Xschm;
import Editor.Xtypes.Xtype;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.LinkedList;

/**
 * Created by Krzysiek on 2016-12-16.
 */
public class ModelCreator {
    private String modelName;
    private LinkedList<Xtype> types = new LinkedList<>();
    private LinkedList<Xattr> attributes = new LinkedList<>();
    private LinkedList<Xschm>  schemes = new LinkedList<>();
    private LinkedList<Xrule> rules = new LinkedList<>();

    public ModelCreator(String modelName){
        this.modelName = modelName;
    }

    public void setModelName(String modelName){
        this.modelName = modelName;
    }
    public void addType(Xtype xtype){
        types.add(xtype);
    }
    public void addAttribute(Xattr xattr){
        attributes.add(xattr);
    }
    public void addScheme(Xschm xschm){
        schemes.add(xschm);
    }
    public void addRule(Xrule xrule){
        rules.add(xrule);
    }


    /*
    private ModelCreator(Builder builder) {
        types = builder.types;
        attributes = builder.attributes;
        schemes = builder.schemes;
        rules = builder.rules;
    }
    /*
    static Builder builder(){
        return new ModelCreator.Builder();
    }
    public static class Builder {
        private LinkedList<Xtype> types = new LinkedList<>();
        private LinkedList<Xattr> attributes = new LinkedList<>();
        private LinkedList<Xschm>  schemes = new LinkedList<>();
        private LinkedList<Xrule> rules = new LinkedList<>();

        Builder addType(Xtype xtype) {
            types.add(xtype);
            return this;
        }
        Builder addAttr(Xattr xattr){
            attributes.add(xattr);
            return this;
        }
        Builder addSchm(Xschm xschm) {
            schemes.add(xschm);
            return this;
        }
        Builder addRule(Xrule xrule){
            rules.add(xrule);
            return this;
        }
        ModelCreator build() throws Exception {
            if(types.isEmpty()) throw new Exception();
            if(attributes.isEmpty()) throw new Exception();
            if(schemes.isEmpty()) throw new Exception();
            if(rules.isEmpty()) throw new Exception();
            return new ModelCreator(this);
        }
    }
    */
    public void printModel(){
        for (Xtype i : types) {
            System.out.println(i.returnStringForModel());
        }
        for (Xattr i : attributes) {
            System.out.println(i.returnStringForModel());
        }
        for (Xschm i : schemes) {
            System.out.println(i.returnStringForModel());
        }
        for (Xrule i : rules) {
            System.out.println(i.returnStringForModel());
        }
    }

    public void saveModel(){
        File file = new File(modelName + ".txt");
        try (PrintWriter out = new PrintWriter(file)) {
            for (Xtype i : types) {
                out.println(i.returnStringForModel());
            }
            for (Xattr i : attributes) {
                out.println(i.returnStringForModel());
            }
            for (Xschm i : schemes) {
                out.println(i.returnStringForModel());
            }
            for (Xrule i : rules) {
                out.println(i.returnStringForModel());
            }
        } catch (IOException e){}



    }
}