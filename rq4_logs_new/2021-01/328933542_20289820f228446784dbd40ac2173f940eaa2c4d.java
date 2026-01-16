package com.mate.dao.jdbcimpl;

import com.mate.dao.ManufacturerDao;
import com.mate.exception.DataProcessingException;
import com.mate.lib.Dao;
import com.mate.model.Manufacturer;
import com.mate.util.ConnectionUtil;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

@Dao
public class ManufacturerDaoJdbcImpl implements ManufacturerDao {
    @Override
    public Manufacturer create(Manufacturer manufacturer) {
        String insertQuery = "INSERT INTO manufacturers (name, country)"
                + "VALUES (?, ?)";
        try (Connection connection = ConnectionUtil.getConnection();
                PreparedStatement preparedStatement =
                        connection.prepareStatement(
                                insertQuery, Statement.RETURN_GENERATED_KEYS)) {
            preparedStatement.setString(1, manufacturer.getName());
            preparedStatement.setString(2, manufacturer.getCountry());
            preparedStatement.executeUpdate();
            ResultSet resultSet = preparedStatement.getGeneratedKeys();
            if (resultSet.next()) {
                manufacturer.setId(resultSet.getObject(1, Long.class));
            }
            return manufacturer;
        } catch (SQLException e) {
            throw new DataProcessingException("Can't get connection in method create", e);
        }
    }

    @Override
    public Optional<Manufacturer> get(Long id) {
        String selectQuery = "SELECT * FROM manufacturers where id = ?";
        try (Connection connection = ConnectionUtil.getConnection();
                 PreparedStatement preparedStatement =
                         connection.prepareStatement(
                                 selectQuery, Statement.RETURN_GENERATED_KEYS)) {
            preparedStatement.setLong(1, id);
            ResultSet resultSet = preparedStatement.executeQuery();
            resultSet.next();
            if (resultSet.getBoolean("deleted")) {
                return Optional.empty();
            }
            return Optional.of(parseFromResultSet(resultSet));
        } catch (SQLException e) {
            throw new DataProcessingException("Can't get connection in method get", e);
        }
    }

    @Override
    public List<Manufacturer> getAll() {
        String selectQuery = "SELECT * FROM manufacturers";
        try (Connection connection = ConnectionUtil.getConnection();
                 PreparedStatement preparedStatement =
                         connection.prepareStatement(selectQuery)) {
            ResultSet resultSet = preparedStatement.executeQuery();
            List<Manufacturer> manufacturers = new ArrayList<>();
            while (resultSet.next()) {
                if (!resultSet.getBoolean("deleted")) {
                    manufacturers.add(parseFromResultSet(resultSet));
                }
            }
            return manufacturers;
        } catch (SQLException e) {
            throw new DataProcessingException("Can't get connection in method getAll", e);
        }
    }

    @Override
    public Manufacturer update(Manufacturer manufacturer) {
        String selectQuery = "UPDATE manufacturers SET name = ?, country = ? WHERE id = ?";
        try (Connection connection = ConnectionUtil.getConnection();
                 PreparedStatement preparedStatement =
                         connection.prepareStatement(
                                 selectQuery, Statement.RETURN_GENERATED_KEYS)) {
            preparedStatement.setString(1, manufacturer.getName());
            preparedStatement.setString(2, manufacturer.getCountry());
            preparedStatement.setLong(3, manufacturer.getId());
            Manufacturer old = get(manufacturer.getId()).get();
            preparedStatement.executeUpdate();
            return old;
        } catch (SQLException e) {
            throw new DataProcessingException("Can't get connection in method get", e);
        }
    }

    @Override
    public boolean delete(Long id) {
        String selectQuery = "UPDATE manufacturers SET deleted = true WHERE id = ?";
        try (Connection connection = ConnectionUtil.getConnection();
                 PreparedStatement preparedStatement =
                         connection.prepareStatement(
                                 selectQuery, Statement.RETURN_GENERATED_KEYS)) {
            preparedStatement.setLong(1, id);
            return preparedStatement.executeUpdate() > 0;
        } catch (SQLException e) {
            throw new DataProcessingException("Can't get connection in method get", e);
        }
    }

    private Manufacturer parseFromResultSet(ResultSet resultSet) throws SQLException {
        long manufacturerId = resultSet.getLong("id");
        String name = resultSet.getNString("name");
        String country = resultSet.getNString("country");
        Manufacturer manufacturer = new Manufacturer(name, country);
        manufacturer.setId(manufacturerId);
        return manufacturer;
    }
}