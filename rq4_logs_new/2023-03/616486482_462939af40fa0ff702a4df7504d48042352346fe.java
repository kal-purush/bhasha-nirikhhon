package blog.dal;

import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.sql.Types;

import blog.model.BlogPosts;
import blog.model.BlogUsers;
import blog.model.PlayList;

public class PlayListDao {
	protected ConnectionManager connectionManager;

	private static PlayListDao instance = null;
	protected PlayListDao() {
		connectionManager = new ConnectionManager();
	}
	public static PlayListDao getInstance() {
		if(instance == null) {
			instance = new PlayListDao();
		}
		return instance;
	}
	public PlayList create(PlayList playList) throws SQLException {
		String insertPlayList =
			"INSERT INTO PlayList(UserId,Created) " +
			"VALUES(?,?);";
		Connection connection = null;
		PreparedStatement insertStmt = null;
		ResultSet resultKey = null;
		try {
			connection = connectionManager.getConnection();
			
			insertStmt = connection.prepareStatement(insertPlayList,
				Statement.RETURN_GENERATED_KEYS);
			insertStmt.setLong(1, playList.getUser().getUserId());
			
			insertStmt.setDate(2, (Date) playList.getCreated());
			
			insertStmt.executeUpdate();
		
			resultKey = insertStmt.getGeneratedKeys();
			int playListId = -1;
			if(resultKey.next()) {
				playListId = resultKey.getInt(1);
			} else {
				throw new SQLException("Unable to retrieve auto-generated key.");
			}
			playList.setPlayListId(playListId);
			return playList;
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
	
	public PlayList delete(PlayList playList) throws SQLException {
		String deletePlayList = "DELETE FROM PlayList WHERE PlayListId=?;";
		Connection connection = null;
		PreparedStatement deleteStmt = null;
		try {
			connection = connectionManager.getConnection();
			deleteStmt = connection.prepareStatement(deletePlayList);
			deleteStmt.setLong(1, playList.getPlayListId());
			deleteStmt.executeUpdate();
			return null;
		} catch (SQLException e) {
			e.printStackTrace();
			throw e;
		} finally {
			if(connection != null) {
				connection.close();
			}
			if(deleteStmt != null) {
				deleteStmt.close();
			}
		}
	}
	
	public PlayList getPlayListById(long playListId) throws SQLException {
		String selectPlayList =
			"SELECT * " +
			"FROM PlayList " +
			"WHERE PlayListId=?;";
		Connection connection = null;
		PreparedStatement selectStmt = null;
		ResultSet results = null;
		try {
			connection = connectionManager.getConnection();
			selectStmt = connection.prepareStatement(selectPlayList);
			selectStmt.setLong(1, playListId);
			results = selectStmt.executeQuery();
			
			BlogUsersDao blogUsersDao = BlogUsersDao.getInstance();
			
			if(results.next()) {
				long PlayListId = results.getLong("PlayListId");
				Date Created = results.getDate("Created");
				
				
				User user =UserDao.getUserFromUserId(results.getLong("UserId"));
				
				PlayList playList = new PlayList(PlayListId,user,Created);
				return playList;
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