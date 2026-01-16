package by.it_academy.team1.usermessages.dao.database;

import by.it_academy.team1.usermessages.dao.api.IStatisticsDao;

import javax.sql.DataSource;
import java.beans.PropertyVetoException;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class DatabaseStatisticsDao implements IStatisticsDao {
    private final DataSource ds = DataSourceCreator.getInstance();

    public DatabaseStatisticsDao() throws PropertyVetoException, SQLException, IOException {
    }

    @Override
    public int getUsersCount() throws SQLException {
        String sql = "SELECT COUNT(*) FROM user_messages.users";
        return getFirstColum(sql);
    }

    @Override
    public int getMessagesCount() throws SQLException {
        String sql = "SELECT COUNT(*) FROM user_messages.messages";
        return getFirstColum(sql);
    }

    private int getFirstColum(String sql) throws SQLException {
        try (Connection conn = ds.getConnection();
             PreparedStatement stm = conn.prepareStatement(sql);
             ResultSet resultSet = stm.executeQuery()) {
            return resultSet.getInt(1);
        }
    }

}