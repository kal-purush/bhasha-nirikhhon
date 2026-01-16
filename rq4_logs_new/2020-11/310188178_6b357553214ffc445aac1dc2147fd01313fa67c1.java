package com.shift.myshift;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;

import javax.servlet.http.HttpServletRequest;

import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.servlet.ModelAndView;

@Controller
public class MyShiftController {

	@RequestMapping(value = "schedule.do", method = RequestMethod.POST)
	public ModelAndView showresult(HttpServletRequest request) {
		ModelAndView mv = new ModelAndView("result");
		// string -> date ���� ���� ����
		DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
		// view�ܿ���  ���� �Ķ���� �� �ޱ�
		int n1 = Integer.parseInt(request.getParameter("n1"));
		int n2 = Integer.parseInt(request.getParameter("n2"));
		int n3 = Integer.parseInt(request.getParameter("n3"));
		String start = request.getParameter("start");
		String end = request.getParameter("end");
		// ���� �Ķ���� �ޱ�(��� �޾ƾ�����, ó���ؾ����� ���� �𸣰���)
		//String birthday = request.getParameter("birthday");
		
		// �ٹ�ǥ ������, ������
		Date startdate,enddate; 
		try {
			startdate = dateFormat.parse(start);
			enddate = dateFormat.parse(end);
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		// �ѿ�
		int k = n1+n2+n3;
		// ��� �迭
		ArrayList<NurseVO> list = new ArrayList<NurseVO>();
		// ����ȣ��� ���밣ȣ�� ����
		for (int i = 0; i < n1; i++) {
			NurseVO headnurse = new NurseVO();
			headnurse.setIdx1(i);
			headnurse.setType("n1");
			list.add(headnurse);
		}
		for (int i = 0; i < (k-n1); i++) {
			NurseVO othernurse = new NurseVO();
			othernurse.setIdx2(i);
			list.add(othernurse);
		}
		
		// �ٹ�ǥ ����
		for (NurseVO m : list) {
			if (m.getType()=="n1") {
				
			}else {
				
			}
		}
		

		
		return mv;
	}
}