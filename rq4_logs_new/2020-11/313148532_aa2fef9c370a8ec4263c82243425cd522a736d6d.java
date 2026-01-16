package model;

public class Country implements Model {
    private final String name;

    public Country(String name) {
        this.name = name;
    }

    public String getName() { return this.name; };

    public String[] tupleToListOfString() {

        String[] tupleValues = { String.valueOf(this.name) };
        return tupleValues;
    }

    public String[] columnNameListOfString() {

        String[] columnNames = { "name" };
        return columnNames;
    }
}


/*
    CREATE TABLE; Country (
        name char(10) PRIMARY KEY
);*/