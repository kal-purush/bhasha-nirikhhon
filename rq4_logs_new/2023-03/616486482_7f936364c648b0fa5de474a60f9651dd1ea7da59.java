package melodic.dal;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import melodic.model.TrackVO;

public class TrackVODao {
  protected ConnectionManager connectionManager;
  private static TrackVODao instance = null;
  protected TrackVODao() {
    connectionManager = new ConnectionManager();
  }
  public static TrackVODao getInstance() {
    if(instance == null) {
      instance = new TrackVODao();
    }
    return instance;
  }


  public List<TrackVO> getTopTen() throws SQLException {
    List<TrackVO> tracks = new ArrayList<>();
    String sql = "SELECT AlbumId, AlbumName,SUM(DurationMS) AS totalTime\n"
        + "FROM Track\n"
        + "GROUP BY AlbumId, AlbumName\n"
        + "ORDER BY totalTime DESC\n"
        + "LIMIT 10;";
    Connection connection = null;
    PreparedStatement selectStmt = null;
    ResultSet result = null;
    try {
      connection = connectionManager.getConnection();
      selectStmt = connection.prepareStatement(sql,
          Statement.RETURN_GENERATED_KEYS);

      result = selectStmt.executeQuery();

      // Retrieve the auto-generated key and set it, so it can be used by the caller.

      while (result.next()) {
        Long albumId = result.getLong("AlbumId");
        String albumName = result.getString("AlbumName");
        Long totalTime = result.getLong("totalTime");
        tracks.add(new TrackVO(albumId, albumName, totalTime));
      }
      System.out.println(tracks);


      return tracks;
    } catch (SQLException e) {
      e.printStackTrace();
      throw e;
    } finally {
      if(connection != null) {
        connection.close();
      }
      if(selectStmt != null) {
        selectStmt.close();
      }
    }
  }


}