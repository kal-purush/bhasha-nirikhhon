package com.sg.supersightings.mapper;

import com.sg.supersightings.dto.Superpower;
import org.springframework.jdbc.core.RowMapper;

import java.sql.ResultSet;
import java.sql.SQLException;

public class SuperpowerMapper implements RowMapper<Superpower> {

    @Override
    public Superpower mapRow(ResultSet rs, int rowNum) throws SQLException {
        Superpower superpower = new Superpower();
        superpower.setSuperpowerId(rs.getInt("superpowerID"));
        superpower.setSuperpowerName(rs.getString("superpowerName"));
        return superpower;
    }
}