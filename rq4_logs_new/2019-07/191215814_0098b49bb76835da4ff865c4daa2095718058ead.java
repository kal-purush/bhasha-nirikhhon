package com.epam.brest.summer.courses2019.dao;

import com.epam.brest.summer.courses2019.model.Car;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.jdbc.support.KeyHolder;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

public class CarDaoJdbcImpl implements CarDao {

    private final NamedParameterJdbcTemplate namedParameterJdbcTemplate;

    private final static String SELECT_ALL =
            "select c.car_id, c.number from car c";

    private final static String ADD_DEPARTMENT = "insert into department (department_name) values (:departmentName)";

    public CarDaoJdbcImpl(NamedParameterJdbcTemplate namedParameterJdbcTemplate) {
        this.namedParameterJdbcTemplate = namedParameterJdbcTemplate;
    }

    @Override
    public Car add(Car car) {

        MapSqlParameterSource parameters = new MapSqlParameterSource();
        parameters.addValue("carModel", car.getCarModel());

        KeyHolder generatedKeyHolder = new GeneratedKeyHolder();
        namedParameterJdbcTemplate.update(ADD_DEPARTMENT, parameters, generatedKeyHolder);
        car.setCarId(generatedKeyHolder.getKey().intValue());
        return car;
    }


    @Override
    public void update(Car car) {

    }

    @Override
    public void delete(Integer carId) {

    }

    @Override
    public List<Car> findAll() {

        List<Car> cars =
                namedParameterJdbcTemplate.query(SELECT_ALL, new CarRowMapper());
        return cars;
    }

    private class CarRowMapper implements RowMapper<Car> {
        @Override
        public Car mapRow(ResultSet resultSet, int i) throws SQLException {
            Car car = new Car();
            car.setCarId(resultSet.getInt("car_id"));
            car.setCarNumber(resultSet.getString("number"));
            return car;
        }
    }
}