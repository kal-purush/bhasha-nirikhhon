package com.boot.Service;

import java.util.List;

import org.apache.ibatis.session.SqlSession;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import com.boot.DAO.AttendanceDAO;
import com.boot.DAO.CouponDAO;
import com.boot.DAO.UsertbDAO_4;
import com.boot.DTO.CouponDTO;
import com.boot.DTO.UsertbDTO;
import com.boot.Security.CustomUserDetails;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@Service("AttendencdService")
public class AttendenceServiceImpl implements AttendencdService {
	@Autowired
	private SqlSession sqlSession;
	
	
	// 출석 기록 추가
	@Override
	public void checkAttendance() {
		log.info("AttendenceService임플 checkAttendance 접근");

		// 사용자 정보 가져오기
		CustomUserDetails userDetails = (CustomUserDetails) SecurityContextHolder.getContext().getAuthentication().getPrincipal();
		String uuid = userDetails.getUuId();  // 사용자 UUID 가져오기
		 // 오늘 출석 체크 여부 확인
		AttendanceDAO dao = sqlSession.getMapper(AttendanceDAO.class);
        int todayAttendance = dao.checkTodayAttendance(uuid);
        if (todayAttendance == 0) {
            dao.insertAttendance(uuid);
        }
	}

	// 출석 만근자 uuid 조회
	@Override
	public List<String> checkMonthlyAttendance() {
		log.info("AttendenceService임플 checkMonthlyAttendance 접근");
		
		AttendanceDAO dao = sqlSession.getMapper(AttendanceDAO.class);
		return dao.checkMonthlyAttendance();
	}
    
    
    
    
}
	
	
