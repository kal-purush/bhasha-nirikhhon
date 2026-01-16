package edu.iss.gitcloneteam.weatherobserver.dao;

import edu.iss.gitcloneteam.weatherobserver.model.Measurement;
import edu.iss.gitcloneteam.weatherobserver.model.MeasurementRowMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.PropertySource;
import org.springframework.core.env.Environment;
import org.springframework.jdbc.core.BeanPropertyRowMapper;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Repository;

import java.sql.Timestamp;
import java.util.List;

@Repository
@PropertySource("classpath:/queries.properties")
public class MeasurementDaoImpl implements MeasurementDao {

    private JdbcTemplate jdbcTemplate;
    private Environment environment;

    @Autowired
    public MeasurementDaoImpl(JdbcTemplate jdbcTemplate, Environment environment) {
        this.jdbcTemplate = jdbcTemplate;
        this.environment = environment;
    }

    @Override
    public Measurement getLastMeasurement() {
        String query = environment.getProperty("select.measurement.last");
        RowMapper<Measurement> rowMapper = new MeasurementRowMapper();
        Measurement lastMeasurement = jdbcTemplate.queryForObject(query, rowMapper);
        return lastMeasurement;
    }

    @Override
    public List<Measurement> getMeasurementForTimeInterval(int hours) {
        String query = environment.getProperty("select.measurements.for.interval");
        RowMapper<Measurement> rowMapper = new BeanPropertyRowMapper<Measurement>(Measurement.class);
        List<Measurement> measurementsForTimeInterval = jdbcTemplate.query(query, rowMapper, hours);
        return measurementsForTimeInterval;
    }

    @Override
    public void addMeasurement(Measurement measurement) {
        String query = environment.getProperty("insert.measurement");
        jdbcTemplate.update(
                query,
                measurement.getTemperature(),
                measurement.getPressure(),
                Timestamp.valueOf(measurement.getMeasurementTime())
        );
    }
}