package HW13;


import org.apache.log4j.Logger;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

public class SQLCompanies {
    private static final Logger LOGGER = Logger.getLogger(SQLCompanies.class);
    private static final String SELECT_ID = "SELECT * FROM companies WHERE id = ?";
    private static final String SELECT_ALL = "SELECT * FROM companies";
    private static final String INSERT = "INSERT INTO companies(company_name, country) VALUES(?, ?)";
    private static final String UPDATE = "UPDATE companies SET company_name = ?, country = ? WHERE id = ?";
    private static final String DELETE = "DELETE FROM companies WHERE id = ?";

    public Company selectId(int id) throws SQLException {
        ResultSet resultSet = null;
        Company company = new Company();
        try (Connection connection = MySQLConnection.createConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(SELECT_ID)) {
            preparedStatement.setInt(1, id);
            resultSet = preparedStatement.executeQuery();
            resultSet.next();
            company.setID(resultSet.getInt(1)).setCompany_name(resultSet.getString(2)).setCountry(resultSet.getString(3));
        } catch (SQLException e) {
            LOGGER.error(e.getMessage());
        } finally {
            resultSet.close();
        }
        return company;
    }

    public List<Company> selectAll() throws SQLException {
        ResultSet resultSet = null;
        List<Company> list = new ArrayList<>();
        try (Connection connection = MySQLConnection.createConnection();
             Statement statement = connection.createStatement()) {
            resultSet = statement.executeQuery(SELECT_ALL);
            while (resultSet.next()) {
                list.add(new Company().setCompany_name(resultSet.getString(2)).setCountry(resultSet.getString(3)));
            }
        } catch (SQLException e) {
            LOGGER.error(e.getMessage());
        }finally {
            resultSet.close();
        }
        return list;
    }

    public void deleteById(int id) throws SQLException {
        PreparedStatement preparedStatement = null;
        try (Connection connection = MySQLConnection.createConnection()) {
            connection.setAutoCommit(false);
            preparedStatement = connection.prepareStatement(DELETE);
            preparedStatement.setInt(1, id);
            preparedStatement.executeUpdate();
            connection.commit();
        } catch (SQLException e) {
            LOGGER.error(e.getMessage());
        }finally {
            preparedStatement.close();
        }
    }

    public void insert(Company company) throws SQLException {
        PreparedStatement preparedStatement = null;
        try (Connection connection = MySQLConnection.createConnection()) {
            connection.setAutoCommit(false);
            preparedStatement = connection.prepareStatement(INSERT);
            preparedStatement.setString(2, company.getCompany_name());
            preparedStatement.setString(3, company.getCountry());
            preparedStatement.executeUpdate();
            connection.commit();
        } catch (SQLException e) {
            LOGGER.error(e.getMessage());
        }finally {
            preparedStatement.close();
        }
    }

    public void update(Company company) throws SQLException {
        PreparedStatement preparedStatement = null;
        try (Connection connection = MySQLConnection.createConnection()) {
            connection.setAutoCommit(false);
            preparedStatement = connection.prepareStatement(UPDATE);
            preparedStatement.setInt(1, company.getID());
            preparedStatement.setString(2, company.getCompany_name());
            preparedStatement.setString(3, company.getCountry());
            preparedStatement.executeUpdate();
            connection.commit();
        } catch (SQLException e) {
            LOGGER.error(e.getMessage());
        }finally {
            preparedStatement.close();
        }
    }
}