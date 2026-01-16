package sisgusol.Xtras;

import com.digi.xbee.api.models.XBeeMessage;
import java.util.Date;
import java.sql.*;

public class Measure {
    private String NodeID;
    private String depth;
    private Float measure;
    private Date dateTime;

    public Measure() {
    }
    
    public String getNodeID() {
        return NodeID;
    }

    public String getDepth() {
        return depth;
    }

    public Float getMeasure() {
        return measure;
    }

    public Date getDateTime() {
        return dateTime;
    }

    public void setNodeID(String NodeID) {
        this.NodeID = NodeID;
    }

    public void setDepth(String depth) {
        this.depth = depth;
    }

    public void setMeasure(Float measure) {
        this.measure = measure;
    }

    public void setDateTime(Date dateTime) {
        this.dateTime = dateTime;
    }
    
    public void createDB (DBInterface database) throws SQLException {
        database.createTable("CREATE TABLE IF NOT EXISTS `sisgusol`.`measurements` ("
                            + " `DATE_TIME` DATETIME NOT NULL ,"
                            + " `NODE_ID` VARCHAR(8) NOT NULL ,"
                            + " `64BIT_ADDRESS` VARCHAR(16) NOT NULL ,"
                            + " `DEPTH` INT NOT NULL ,"
                            + " `MEASURE` FLOAT NOT NULL ,"
                            + " PRIMARY KEY (`DATE_TIME`, `NODE_ID`));");
    }
    
    public void insertIntoDB (DBInterface database, XBeeMessage xbm, Date dateTime) throws SQLException {
        //String data[2] = 
        
        
        database.insert("INSERT INTO `sisgusol`.`measurements` (`DATE_TIME`,`NODE_ID`,`64BIT_ADDRESS`,`DEPTH`,`MEASURE`)"+"("
                        +dateTime+","
                        +xbm.getDevice().getNodeID()+","
                        +xbm.getDevice().get64BitAddress().toString()+","
                        +
             ")");
    }
    
    
}