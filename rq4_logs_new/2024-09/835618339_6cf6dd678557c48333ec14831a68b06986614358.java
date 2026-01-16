package com.kh.erp.mapper;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Service;

import com.kh.erp.VO.AttendanceSummaryVO;

@Service
public class AttendanceSummaryMapper implements RowMapper<AttendanceSummaryVO> {

	@Override
	public AttendanceSummaryVO mapRow(ResultSet rs, int rowNum) throws SQLException {
		
		AttendanceSummaryVO attendanceSummaryVO = new AttendanceSummaryVO();
		
		attendanceSummaryVO.setLoginId(rs.getString("loginId"));
		attendanceSummaryVO.setTbYear(rs.getInt("tb_year"));
		attendanceSummaryVO.setTbMonth(rs.getInt("tb_month"));
		attendanceSummaryVO.setDaysWorked(rs.getInt("days_worked"));
        
        // 컴마 기준으로 잘라다가 list로 저장
        String checkInDates = rs.getString("check_in_dates");
        List<String> datesList = Arrays.asList(checkInDates.split(", "));
        attendanceSummaryVO.setCheckInDates(datesList);
        
        attendanceSummaryVO.setTotalWorkTime(rs.getString("total_work_time"));
		
		return attendanceSummaryVO;
	}

}