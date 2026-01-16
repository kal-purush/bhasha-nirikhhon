package model;

public class Includes {
    private final int houseNum;
    private final String streetName;
    private final String postalCode;
    private final String cname;
    private final int routeID;
    private final String time;

    public Includes(int houseNum, String streetName, String postalCode, String cname, int routeID, String time) {
        this.houseNum = houseNum;
        this.streetName = streetName;
        this.postalCode = postalCode;
        this.cname = cname;
        this.routeID = routeID;
        this.time = time;
    }

    public int getHouseNum() { return this.houseNum; }
    public String getStreetName() { return this.streetName; }
    public String getPostalCode() { return this.postalCode; }
    public String getCname() { return this.cname; }
    public int getRouteID() { return this.routeID; }
    public String getTime() { return this.time; }

    public String[] tupleToListOfString() {

        String[] tupleValues = { String.valueOf(this.houseNum), this.streetName, this.postalCode, this.cname, String.valueOf(this.routeID), this.time};
        return tupleValues;
    }

    public String[] columnNameListOfString() {

        String[] columnNames = { "houseNum", "streetName", "postalCode", "cname", "routeID", "time" };
        return columnNames;
    }
}

/*
CREATE TABLE Includes (
	houseNum INTEGER,
	streetName CHAR(20),
	postalCode CHAR(10),
	cname CHAR(20),
	routeID INTEGER,
	time TIMESTAMP,
	PRIMARY KEY (houseNum, streetName, postalCode, cname, routeID)
	FOREIGN KEY (houseNum, streetName, postalCode) REFERENCES Place
	FOREIGN KEY (cname) REFERENCES Country (name)
	FOREIGN KEY (routeID) REFERENCES Route
);

 */