package com.sg.supersightings.mappers;

import com.sg.supersightings.dto.Location;
import org.springframework.jdbc.core.RowMapper;

import java.sql.ResultSet;
import java.sql.SQLException;

public class LocationMapper implements RowMapper<Location> {

    @Override
    public Location mapRow(ResultSet rs, int rowNum) throws SQLException {
        Location location = new Location();

        location.setLocationID(rs.getInt("locationID"));
        location.setLocationDesc(rs.getString("locationDesc"));
        location.setLocationAddress(rs.getString("locationAddress"));
        location.setLatitude(rs.getString("latitude"));
        location.setLongitude(rs.getString("longitude"));

        return location;
    }
}