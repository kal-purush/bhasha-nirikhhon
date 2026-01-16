package com.aurospaces.neighbourhood.controller;

import java.io.IOException;
import java.util.List;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpSession;

import org.apache.commons.lang.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.ModelAttribute;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.servlet.mvc.support.RedirectAttributes;

import com.aurospaces.neighbourhood.bean.AccessoriesmasterBean;
import com.aurospaces.neighbourhood.bean.CustomermasterBean;
import com.aurospaces.neighbourhood.bean.KhaibarUsersBean;
import com.aurospaces.neighbourhood.db.dao.KhaibarUsersDao;
import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

@Controller
@RequestMapping(value="/admin")
public class UserManagementController {
	
	@Autowired	KhaibarUsersDao khaibarusersDao;
	
	@RequestMapping(value = "/usermanagement")
	public String usermanagement(@ModelAttribute("usermanagementForm")KhaibarUsersBean khaibarusersBean,HttpServletRequest request,
			HttpSession session) {
		
		ObjectMapper objectMapper = null;
		String sJson = null;
		List<CustomermasterBean> customerList=null;
		try {
			
			sJson=khaibarusersDao.getAllUsers("1");
			if(sJson !=null){
				
				 request.setAttribute("allObjects", sJson);
			}else{
				
				request.setAttribute("allObjects", "''");
			}
			
			
			

		} catch (Exception e) {
			e.printStackTrace();
			System.out.println(e);

		}
		

	return "usermanagement";
	
}

@SuppressWarnings("null")
@RequestMapping(value = "/usermanagement", method = RequestMethod.POST)
public  String userSave(@ModelAttribute("usermanagementForm")KhaibarUsersBean khaibarusersBean, HttpSession objSession,HttpServletRequest objRequest,RedirectAttributes redirect) {
	KhaibarUsersBean objKhaibarusersbean= null;
	int id=0;
	try {
		String sName=khaibarusersBean.getUserName();
		objKhaibarusersbean=khaibarusersDao.getByUserName(sName);
		int dummyId =0;
		if(objKhaibarusersbean != null){
			dummyId = objKhaibarusersbean.getId();
		}
		if(khaibarusersBean.getId() != 0)
		{
			id = khaibarusersBean.getId();
			if(id == dummyId || objKhaibarusersbean == null )
			{
				if(StringUtils.isBlank(khaibarusersBean.getEdit()) ||  khaibarusersBean.getEdit().equals("0")){
					khaibarusersBean.setEdit("0");
				}
				if(StringUtils.isBlank(khaibarusersBean.getMobileapp()) ||  khaibarusersBean.getMobileapp().equals("0")){
					khaibarusersBean.setMobileapp("0");
				}
				if(StringUtils.isBlank(khaibarusersBean.getDelete1()) ||  khaibarusersBean.getDelete1().equals("0")){
					khaibarusersBean.setDelete1("0");
				}
				khaibarusersDao.save(khaibarusersBean);
				redirect.addFlashAttribute("msg", "Record Updated Successfully");
				redirect.addFlashAttribute("cssMsg", "warning");
			}
			else
			{
				redirect.addFlashAttribute("msg", "Already Record Exist");
				redirect.addFlashAttribute("cssMsg", "danger");
			}
		}
		if(khaibarusersBean.getId() == 0 && objKhaibarusersbean == null)
		{
			khaibarusersBean.setStatus("1");
			khaibarusersBean.setRoleId("2");
			if(StringUtils.isBlank(khaibarusersBean.getEdit()) ||  khaibarusersBean.getEdit().equals("0")){
				khaibarusersBean.setEdit("0");
			}
			if(StringUtils.isBlank(khaibarusersBean.getMobileapp()) ||  khaibarusersBean.getMobileapp().equals("0")){
				khaibarusersBean.setMobileapp("0");
			}
			if(StringUtils.isBlank(khaibarusersBean.getDelete1()) ||  khaibarusersBean.getDelete1().equals("0")){
				khaibarusersBean.setDelete1("0");
			}
			khaibarusersDao.save(khaibarusersBean);
			
			redirect.addFlashAttribute("msg", "Record Inserted Successfully");
			redirect.addFlashAttribute("cssMsg", "success");
		}
		if(khaibarusersBean.getId() == 0 && objKhaibarusersbean != null)
		{
			redirect.addFlashAttribute("msg", "Already Record Exist");
			redirect.addFlashAttribute("cssMsg", "danger");
		}
	} catch (Exception e) {
		System.out.println("Exception in user Management Controller in userSave()");
		e.printStackTrace();
	}
	return "redirect:usermanagement";
}

@RequestMapping(value = "/userDelete")
public @ResponseBody String customerDelete( @RequestParam("id") String id,@RequestParam("status") String status, HttpSession objSession,
		HttpServletRequest objRequest) throws JsonGenerationException, JsonMappingException, IOException {
	boolean isDelete = false;
	String sJson = "";
	Boolean accessoriesmasterBean=null;
	List<AccessoriesmasterBean> accessories=null;
	ObjectMapper objectMapper = null;
	  int dId=Integer.parseInt(id);
	  isDelete = khaibarusersDao.delete(dId,status);
	 
	  if(isDelete){
		  sJson=khaibarusersDao.getAllUsers("1");
		  System.out.println("deleted cusmer data--"+sJson);
			
		}
	
	
	return sJson;
}
@RequestMapping(value = "/inActiveUser")
public @ResponseBody String inActiveCustomer( @RequestParam("status") String status, HttpSession objSession,
		HttpServletRequest objRequest) throws JsonGenerationException, JsonMappingException, IOException {
	boolean isDelete = false;
	String sJson = "";
	 try{
		  sJson=khaibarusersDao.getAllUsers(status);
		  System.out.println("deleted cusmer data--"+sJson);
	
	 }catch(Exception e){
		 e.printStackTrace();
	 }
	return sJson;
}
}
