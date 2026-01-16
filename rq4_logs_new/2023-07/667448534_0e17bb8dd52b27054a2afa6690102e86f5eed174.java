package com.sg.supersightings.mapper;


import com.sg.supersightings.dto.Super;
import org.springframework.jdbc.core.RowMapper;

import java.sql.ResultSet;
import java.sql.SQLException;


public class SuperMapper implements RowMapper<Super> {

    @Override
    public Super mapRow(ResultSet rs, int rowNum) throws SQLException {
        Super mapperSuper = new Super();
        mapperSuper.setSuperID(rs.getInt("superID"));
        mapperSuper.setSuperName(rs.getString("superName"));
        mapperSuper.setSuperDescription(rs.getString("superDesc"));
        mapperSuper.setEvil(rs.getBoolean("isEvil"));
        mapperSuper.setSuperID(rs.getInt("superpowerID"));
        return mapperSuper;
    }
}