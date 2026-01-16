package model;

public class CnameMedicineID implements Model{
    private final String cname;
    private final int medicineID;

    public CnameMedicineID(String cname, int medicineID) {
        this.cname = cname;
        this.medicineID = medicineID;
    }

    public String getCname() { return this.cname; };
    public int getMedicineID() { return this.medicineID; };


    public String[] tupleToListOfString() {

        String[] tupleValues = { this.cname, String.valueOf(this.medicineID)};
        return tupleValues;
    }

    public String[] columnNameListOfString() {

        String[] columnNames = { "country name", "medicine ID" };
        return columnNames;
    }
}