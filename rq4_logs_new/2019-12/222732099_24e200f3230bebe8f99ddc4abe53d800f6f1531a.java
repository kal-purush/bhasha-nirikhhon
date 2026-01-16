package com.wednesday.crud.controller;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;

import com.wednesday.crud.bean.Department;
import com.wednesday.crud.bean.Msg;
import com.wednesday.crud.service.DepartmentService;

@Controller
public class DepartmentController {

	@Autowired
	DepartmentService departmentService;
	
	
	@RequestMapping("/depts")
	@ResponseBody
	public Msg getDeptsWithJSON() {
		List<Department> depts = departmentService.getAll();
		return Msg.success().add("depts", depts);
	}
}