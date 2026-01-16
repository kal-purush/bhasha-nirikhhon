package org.tms.db.localdb;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.ConnectException;
import java.sql.Connection;
import java.sql.SQLException;
import java.text.DecimalFormat;
import java.util.UUID;

public class RawDataDAO extends BaseDao {
    private final Logger log = LoggerFactory.getLogger(RawDataDAO.class);
    public RawDataDAO() throws ConnectException {
        super();
    }

    public RawDataDAO(Connection conn) {
        super(conn);
    }

    public void addRawData(int count, double avgSpeed, String timeStamp, String facility, String facilityType) {
        try {
            log.info("insert data");

            DecimalFormat df = new DecimalFormat("##.00");
            avgSpeed = Double.valueOf(df.format(avgSpeed));
            log.debug("avgSpeed: " + avgSpeed);
            String insertRawDataSQL = "insert into rawdata(ID,COUNT,SPEED,TIMESTAMP,FACILITY,FACILITY_TYPE)values(?,?,?,?,?,?);";
            prepareStatement(insertRawDataSQL);

            String rawDataId = UUID.randomUUID().toString();

            setString(1, rawDataId);
            setInt(2, count);
            setDouble(3, avgSpeed);
            setString(4, timeStamp);
            setString(5, facility);
            setString(6, facilityType);

            int st = executeUpdate();

            log.debug(" executeUpdate st 1: " + st);

            String insertSyncTableSQL = "insert into sync(RAW_DATA_ID, SYNCED)values(?,?);";
            prepareStatement(insertSyncTableSQL);

            setString(1, rawDataId);
            setInt(2, 0);

            int st2 = executeUpdate();

            log.debug(" executeUpdate st 2 : " + st2);
            
        } catch (SQLException e) {
            log.error("SQLException : " + e.getMessage());
        } finally {
            try {
                log.debug("Closing resources...");
                closeResources();
                log.debug("Closing connection...");
                closeConnection();
            } catch (Exception e) {
            }
        }
    }

}