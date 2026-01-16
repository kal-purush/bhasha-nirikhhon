package melodic.dal;
import melodic.model.Album;
import melodic.model.PlayList;

import java.sql.*;

public class AlbumDao {
    protected ConnectionManager connectionManager;

    private static AlbumDao instance = null;
    protected AlbumDao() {
        connectionManager = new ConnectionManager();
    }
    public static AlbumDao getInstance() {
        if(instance == null) {
            instance = new AlbumDao();
        }
        return instance;
    }

    // create Album given name
    public Album create(Album album) throws SQLException {
        String insertAlbum =
                "INSERT INTO Album(AlbumName) " +
                        "VALUES(?);";
        Connection connection = null;
        PreparedStatement insertStmt = null;
        ResultSet resultKey = null;
        try {
            connection = connectionManager.getConnection();

            insertStmt = connection.prepareStatement(insertAlbum,
                    Statement.RETURN_GENERATED_KEYS);
            insertStmt.setString(1, album.getAlbumName());
            insertStmt.executeUpdate();

            resultKey = insertStmt.getGeneratedKeys();
            int albumId = -1;
            if(resultKey.next()) {
                albumId = resultKey.getInt(1);
            } else {
                throw new SQLException("Unable to retrieve auto-generated key.");
            }
            album.setAlbumId(albumId);
            return album;
        } catch (SQLException e) {
            e.printStackTrace();
            throw e;
        } finally {
            if(connection != null) {
                connection.close();
            }
            if(insertStmt != null) {
                insertStmt.close();
            }
            if(resultKey != null) {
                resultKey.close();
            }
        }
    }




    public Album getAlbumByName(String albumName) throws SQLException {
        String selectCreditCards =
                "SELECT * FROM Album WHERE AlbumName=?;";
        Connection connection = null;
        PreparedStatement selectStmt = null;
        ResultSet results = null;
        try {
            connection = connectionManager.getConnection();
            selectStmt = connection.prepareStatement(selectCreditCards);
            selectStmt.setString(1, albumName);
            results = selectStmt.executeQuery();
            while(results.next()) {
                int id = results.getInt("AlbumId");
                String name = results.getString("AlbumName");
                return new Album(id, name);
            }
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
            if(results != null) {
                results.close();
            }
        }
        return null;
    }
}