package com.getmoney.web.serviceimpls;


import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.getmoney.web.domains.EmpDTO;
import com.getmoney.web.mappers.EmpMapper;
import com.getmoney.web.services.EmpService;

@Service
public class EmpServiceImpl implements EmpService {
	
	@Autowired EmpMapper mapper;
	
	@Override
	public EmpDTO login(EmpDTO param) {
		
	return null;
	}

	@Override
	public boolean join(EmpDTO param) {
		return false;
	}

}