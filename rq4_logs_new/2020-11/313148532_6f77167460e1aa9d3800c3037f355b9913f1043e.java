package model;

public class Kills implements Model {
    private final int medicineID;
    private final int virusID;

    public Kills(int medicineID, int virusID) {
        this.medicineID = medicineID;
        this.virusID = virusID;
    }

    public int getMedicineID() { return this.medicineID; }

    public int getVirusID() { return this.virusID; }

    @Override
    public String[] tupleToListOfString() {

        String[] tupleValues = { String.valueOf(this.medicineID), String.valueOf(this.virusID) };
        return tupleValues;
    }

    @Override
    public String[] columnNameListOfString() {
        String[] columnNames = { "medicineID", "virusID" };
        return columnNames;
    }
}