package com.example;

import org.greenrobot.greendao.generator.DaoGenerator;
import org.greenrobot.greendao.generator.Entity;
import org.greenrobot.greendao.generator.Schema;

public class Generator{

    private static final String PROJECT_DIR = System.getProperty("user.dir");
    private static final int DB_VERSION = 0;
    private static final String DEFAULT_PACKAGE = "com.nupuit.nupuitcontactlist.db";

    public static void main(String [] args){
        Schema schema = new Schema(DB_VERSION, DEFAULT_PACKAGE);
        schema.enableKeepSectionsByDefault();

        addTables(schema);

        try {
            new DaoGenerator().generateAll(schema, PROJECT_DIR  + "//app//src//main//java");
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    private static void addTables(Schema schema){
        addContact(schema);
    }

    private static Entity addContact(Schema schema){
        Entity contacts = schema.addEntity("Contacts");
        contacts.addLongProperty("id").primaryKey().autoincrement();
        contacts.addStringProperty("name");
        contacts.addStringProperty("number");
        contacts.addStringProperty("uri");

        return contacts;

    }
}