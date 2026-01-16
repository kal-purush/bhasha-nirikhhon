package com.sg.supersightings.mapper;

import com.sg.supersightings.dto.Organization;
import org.springframework.jdbc.core.RowMapper;

import java.sql.ResultSet;
import java.sql.SQLException;

public class OrganizationMapper implements RowMapper<Organization> {
    @Override
    public Organization mapRow(ResultSet rs, int rowNum) throws SQLException {
        Organization org = new Organization();
        org.setOrgName(rs.getString("orgName"));
        org.setOrgDes(rs.getString("orgDes"));
        org.setOrgAddress(rs.getString("orgAddress"));
        org.setOrgNumber(rs.getString("orgNumber"));
        org.setEvil(rs.getBoolean("isEvil"));
        return org;
    }
}