package com.boot.Service;


import java.util.List;

import org.apache.ibatis.session.SqlSession;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.authentication.AnonymousAuthenticationToken;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import com.boot.DAO.GradeDAO_4;
import com.boot.DAO.MileageDAO_4;
import com.boot.DAO.PointDAO_4;
import com.boot.DAO.PtHisttbDAO_4;
import com.boot.DTO.MembershipDTO;
import com.boot.DTO.MileageDTO;
import com.boot.DTO.PointDTO;
import com.boot.DTO.PthistDTO;
import com.boot.Security.CustomUserDetails;

import lombok.extern.slf4j.Slf4j;

@Service("MembershipService")
@Slf4j
public class MembershipServiceImpl implements MembershipService {
	@Autowired
	private SqlSession sqlSession;
	
	@Override
	//트랜잭션 내에서 성공적으로 모든 작업이 완료되면, 트랜잭션은 커밋
	//트랜잭션 내에서 예외가 발생하면, 그 시점까지의 모든 작업이 롤백되어, 데이터베이스 상태가 메서드 호출 전 상태로 복원
	@Transactional(rollbackFor = Exception.class)  // 체크된 예외(SQLException, IOException 등)에서도 롤백되도록 설정
	public void insertMembership(String uuid) {
	    log.info("insertMembership 서비스임플");

	    try {
	        // 1. 사용자 grade 생성 - default Welcome
	        GradeDAO_4 dao = sqlSession.getMapper(GradeDAO_4.class);
	        dao.insertGrade(uuid); 

	        // 2. 가입축하 포인트 적립
	        PtHisttbDAO_4 dao2 = sqlSession.getMapper(PtHisttbDAO_4.class);
	        PthistDTO dto = new PthistDTO();
	        dto.setDescription("가입축하포인트 적립"); 
	        dto.setKind("적립"); 
	        dto.setRecqt(1000); // 가입축하포인트 
	        dto.setUuid(uuid); 
	        dao2.updatePtHis(dto); 

	        // 3. 사용자 point 생성 
	        PointDAO_4 dao3 = sqlSession.getMapper(PointDAO_4.class);
	        dao3.insertPoint(uuid); 

	        // 4. 사용자 mileage 생성 
	        MileageDAO_4 dao4 = sqlSession.getMapper(MileageDAO_4.class);
	        dao4.insertMileage(uuid);

	    } catch (Exception e) {
	        log.error("예외 발생: ", e);
	        throw new RuntimeException("회원가입 중 문제가 발생했습니다.", e);
	    }
	}


	@Override
	@Transactional(rollbackFor = Exception.class)
	public MembershipDTO getMembership() {//멤버십 조회(등급, 포인트, 마일리지)
		log.info("getMembership 서비스임플");
		
		// 1. 로그인 여부 확인
	    Authentication authentication = SecurityContextHolder.getContext().getAuthentication();
	    
	    if (authentication == null || !authentication.isAuthenticated() || authentication instanceof AnonymousAuthenticationToken) {
	        throw new RuntimeException("로그인된 사용자가 없습니다.");
	    }
	    
	    // 2. 로그인된 사용자 정보 가져오기
		CustomUserDetails userDetails = (CustomUserDetails) SecurityContextHolder.getContext().getAuthentication().getPrincipal();
    	String uuid = userDetails.getUuId();  // 사용자 ID 가져오기

	    try {
	        // 1. 사용자 grade 조회
	    	GradeDAO_4 dao = sqlSession.getMapper(GradeDAO_4.class);
			String grade = dao.getUserGrade(uuid); 

	        // 2. 포인트 이력조회
			PtHisttbDAO_4 dao2 = sqlSession.getMapper(PtHisttbDAO_4.class);
			List<PthistDTO> pthist = dao2.getUserPtHis(uuid);

	        // 3. 사용자 point 조회 
	        PointDAO_4 dao3 = sqlSession.getMapper(PointDAO_4.class);
	        PointDTO point =dao3.getUserPoint(uuid); 

	        // 4. 사용자 mileage 조회 
	        MileageDAO_4 dao4 = sqlSession.getMapper(MileageDAO_4.class);
	        MileageDTO mileage = dao4.getUserMileage(uuid);
	        
	        // 5. 조회한 정보 멤버십DTO에 담에 반환하기
	        MembershipDTO result = new MembershipDTO(grade,pthist,point,mileage);
	        return result;
	        

	    } catch (Exception e) {
	        log.error("예외 발생: ", e);
	        throw new RuntimeException("멤버십 조회 중 문제가 발생했습니다.", e);
	    }
	}
    	
    	
	


	@Override
	public void updateMembership() {//포인트 업데이트(포인트이력이 생기면 등급, 포인트, 마일리지이 함께 변동됨)
		log.info("updateMembership 서비스임플");
		
		// 1. 로그인 여부 확인
	    Authentication authentication = SecurityContextHolder.getContext().getAuthentication();
	    
	    if (authentication == null || !authentication.isAuthenticated() || authentication instanceof AnonymousAuthenticationToken) {
	        throw new RuntimeException("로그인된 사용자가 없습니다.");
	    }
	    
	    // 2. 로그인된 사용자 정보 가져오기
		CustomUserDetails userDetails = (CustomUserDetails) SecurityContextHolder.getContext().getAuthentication().getPrincipal();
    	String uuid = userDetails.getUuId();  // 사용자 ID 가져오기

	    try {
	        // 1. 사용자 grade 조회
	    	GradeDAO_4 dao = sqlSession.getMapper(GradeDAO_4.class);
			String grade = dao.getUserGrade(uuid); 

	        // 2. 포인트 이력조회
			PtHisttbDAO_4 dao2 = sqlSession.getMapper(PtHisttbDAO_4.class);
			List<PthistDTO> pthist = dao2.getUserPtHis(uuid);

	        // 3. 사용자 point 조회 
	        PointDAO_4 dao3 = sqlSession.getMapper(PointDAO_4.class);
	        PointDTO point =dao3.getUserPoint(uuid); 

	        // 4. 사용자 mileage 조회 
	        MileageDAO_4 dao4 = sqlSession.getMapper(MileageDAO_4.class);
	        MileageDTO mileage = dao4.getUserMileage(uuid);
	        
	        // 5. 조회한 정보 멤버십DTO에 담에 반환하기
	        MembershipDTO result = new MembershipDTO(grade,pthist,point,mileage);
	        

	    } catch (Exception e) {
	        log.error("예외 발생: ", e);
	        throw new RuntimeException("포인트 업데이트 중 문제가 발생했습니다.", e);
	    }
	}



	@Override
	public void deleteMembership() {//포인트 삭제(탈퇴할 때 등급, 포인트, 마일리지이 함께 삭제)
		log.info("deleteMembership 서비스임플");
		
		// 1. 로그인 여부 확인
	    Authentication authentication = SecurityContextHolder.getContext().getAuthentication();
	    
	    if (authentication == null || !authentication.isAuthenticated() || authentication instanceof AnonymousAuthenticationToken) {
	        throw new RuntimeException("로그인된 사용자가 없습니다.");
	    }
	    
	    // 2. 로그인된 사용자 정보 가져오기
		CustomUserDetails userDetails = (CustomUserDetails) SecurityContextHolder.getContext().getAuthentication().getPrincipal();
    	String uuid = userDetails.getUuId();  // 사용자 ID 가져오기

	    try {
	        // 1. 사용자 grade 삭제
	    	GradeDAO_4 dao = sqlSession.getMapper(GradeDAO_4.class);
			dao.deleteGrade(uuid); 

	        // 2. 포인트 이력 삭제
			PtHisttbDAO_4 dao2 = sqlSession.getMapper(PtHisttbDAO_4.class);
			dao2.deletePtHis(uuid);

	        // 3. 사용자 point 삭제
	        PointDAO_4 dao3 = sqlSession.getMapper(PointDAO_4.class);
	        dao3.deletePoint(uuid); 

	        // 4. 사용자 mileage 삭제 
	        MileageDAO_4 dao4 = sqlSession.getMapper(MileageDAO_4.class);
	        dao4.deleteMileage(uuid);
	        

	    } catch (Exception e) {
	        log.error("예외 발생: ", e);
	        throw new RuntimeException("포인트 업데이트 중 문제가 발생했습니다.", e);
	    }
	}
	


	
	
}