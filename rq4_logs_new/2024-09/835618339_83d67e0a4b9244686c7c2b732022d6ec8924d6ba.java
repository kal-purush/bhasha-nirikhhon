package com.kh.erp.service;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import com.kh.erp.dao.TbEmpDao;

@Service
public class ScheduledTasks {

	@Autowired
	private TbEmpDao tbEmpDao;
	
	@Scheduled(cron = "0 0 0 1 1 ?")
//	@Scheduled(fixedRate = 5000) //  테스트
    public void performAnnualTask() {
		//매년 1월 1일 마다 자기가 가진 연차를 15일로 갱신만 하는 작업
		tbEmpDao.updateAnnualLeave();
    }
}