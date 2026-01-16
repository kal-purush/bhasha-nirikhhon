package uia.dao;

import java.sql.ResultSet;
import java.sql.SQLException;

/**
*
* @author Kyle K. Lin
*
*/
public interface DaoColumnReader {

    public Object read(ResultSet rs, int index) throws SQLException;

    /**
     * Returns a string or "" if null.
     *
     * @param rs The result.
     * @param index The index.
     * @return Result.
     * @throws SQLException Failed to read the value.
     */
    public static Object null2Empty(ResultSet rs, int index) throws SQLException {
        String r = rs.getString(index);
        return r == null ? "" : r;
    }

    public static Object empty2Null(ResultSet rs, int index) throws SQLException {
        String r = rs.getString(index);
        return r != null && r.isEmpty() ? null : r;
    }
}