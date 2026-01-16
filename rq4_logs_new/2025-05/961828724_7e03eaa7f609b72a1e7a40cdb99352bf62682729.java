package com.project.quanlycanghangkhong.dao;

import com.project.quanlycanghangkhong.dto.ScheduleDTO;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.BeanPropertyRowMapper;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.time.LocalDate;
import java.util.List;

@Repository
public class ScheduleDAOImpl implements ScheduleDAO {
    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Override
    public List<ScheduleDTO> getSchedulesByUserAndDateRange(Integer userId, LocalDate startDate, LocalDate endDate) {
        // Lấy ca trực thông thường
        String sqlCa = "SELECT us.id AS scheduleId, us.shift_date AS shiftDate, u.id AS userId, u.name AS userName, " +
                "t.id AS teamId, t.team_name AS teamName, un.id AS unitId, un.unit_name AS unitName, " +
                "sh.id AS shiftId, sh.shift_code AS shiftCode, sh.start_time AS startTime, sh.end_time AS endTime, " +
                "sh.location AS location, sh.description AS description " +
                "FROM user_shifts us " +
                "JOIN users u ON us.user_id = u.id " +
                "JOIN shifts sh ON us.shift_id = sh.id " +
                "LEFT JOIN teams t ON u.team_id = t.id " +
                "LEFT JOIN units un ON u.unit_id = un.id " +
                "WHERE us.user_id = ? AND us.shift_date BETWEEN ? AND ? ";
        System.out.println("[LOG] SQL: " + sqlCa + " | Params: " + userId + ", " + startDate + ", " + endDate);
        List<ScheduleDTO> caList = jdbcTemplate.query(
                sqlCa,
                new Object[]{userId, startDate, endDate},
                new BeanPropertyRowMapper<>(ScheduleDTO.class)
        );
        System.out.println("[LOG] Result caList: " + caList);
        // Lấy ca trực chuyến bay
        String sqlFlight = "SELECT ufs.id AS scheduleId, ufs.shift_date AS shiftDate, u.id AS userId, u.name AS userName, " +
                "t.id AS teamId, t.team_name AS teamName, un.id AS unitId, un.unit_name AS unitName, " +
                "NULL AS shiftId, NULL AS shiftCode, NULL AS startTime, NULL AS endTime, " +
                "f.flight_number AS location, CONCAT('Phục vụ chuyến bay ', f.flight_number) AS description " +
                "FROM user_flight_shifts ufs " +
                "JOIN users u ON ufs.user_id = u.id " +
                "JOIN flights f ON ufs.flight_id = f.id " +
                "LEFT JOIN teams t ON u.team_id = t.id " +
                "LEFT JOIN units un ON u.unit_id = un.id " +
                "WHERE ufs.user_id = ? AND ufs.shift_date BETWEEN ? AND ? ";
        System.out.println("[LOG] SQL: " + sqlFlight + " | Params: " + userId + ", " + startDate + ", " + endDate);
        List<ScheduleDTO> flightList = jdbcTemplate.query(
                sqlFlight,
                new Object[]{userId, startDate, endDate},
                new BeanPropertyRowMapper<>(ScheduleDTO.class)
        );
        System.out.println("[LOG] Result flightList: " + flightList);
        // Gộp cả hai loại lịch trực
        caList.addAll(flightList);
        System.out.println("[LOG] Result caList + flightList: " + caList);
        return caList;
    }
}