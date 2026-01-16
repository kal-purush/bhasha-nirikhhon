package com.webside.village.controller;

import javax.servlet.http.HttpServletRequest;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.RequestMapping;

import com.webside.base.basecontroller.BaseController;
import com.webside.exception.AjaxException;
import com.webside.util.Common;
import com.webside.util.PageUtil;
import com.webside.village.mapper.VillageMapper;
import com.webside.village.model.Village;

@Controller
@Scope("prototype")
@RequestMapping("/village/")
public class VillageController extends BaseController{
	@Autowired 
	VillageMapper villageMapper;
	
	@RequestMapping("add.html")
	public String addUI(Model model, HttpServletRequest request) {
		Village v=new Village();
		v.setvAddress("地址");
		v.setvDesc("描述");
		v.setvInuseFlag("1");
		v.setvName("名称");
		v.setvPhone("电话");
		v.setvLogo("logo");
		villageMapper.insert(v);
		return null;
	}
}