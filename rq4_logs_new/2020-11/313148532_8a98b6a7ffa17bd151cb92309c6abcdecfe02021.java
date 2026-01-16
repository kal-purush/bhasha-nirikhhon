package model;

public class NotInfected implements Model{

    private final String nationality;
    private final int sinum;

    public NotInfected(String nationality, int sinum) {
        this.nationality = nationality;
        this.sinum = sinum;
    }

    public String getNationality() { return this.nationality; }
    public int getSinum() { return this.sinum; }

    @Override
    public String[] tupleToListOfString() {
        String[] tupleValues = { this.nationality, String.valueOf(this.sinum) };
        return tupleValues;
    }

    @Override
    public String[] columnNameListOfString() {
        String[] columnNames = { "nationality", "sinum" };
        return columnNames;
    }
}


//    CREATE TABLE NotInfected (
//        nationality char(20),
//        sinum INTEGER,
//    PRIMARY KEY (nationality, sinum),
//    FOREIGN KEY (nationality, sinum) REFERENCES Person (nationality, sinum) ON DELETE CASCADE
//        );