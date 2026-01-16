package com.base.common.email.controller;


import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.ModelAttribute;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.SessionAttributes;
import org.springframework.web.servlet.ModelAndView;

import com.base.common.email.entity.EmailReceiver;
import com.base.common.email.service.EmailReceiverService;
import com.base.common.util.DateUtils;
import com.base.common.util.PageTools;
import com.base.crm.users.entity.UserInfo;

@Controller
@RequestMapping(value = "/emailReceiver")
@SessionAttributes("user")
public class EmailReceiverController {
	private final Logger logger = LoggerFactory.getLogger(getClass());
	@Autowired
	private EmailReceiverService emailReceiverService;
	
	private static final String PRIMARY_MODAL_VIEW = "page/email/receiver/ModifyModal";
	private static final String LOAD_PAGE_VIEW = "page/email/receiver/Content :: container-fluid";
	
	@RequestMapping(value = "/primaryModalView")
	public String primaryModalView(Long id, String modifyModel, Model model) throws Exception {
		logger.debug("primaryModalView request:" + id + ",model:" + model);
		if (id != null) {
			EmailReceiver result = emailReceiverService.selectByPrimaryKey(id);
			model.addAttribute("modifyInfo", result);
		} 

		model.addAttribute("modifyModel", modifyModel);
		logger.debug("primaryModalView model : " + model);

		return PRIMARY_MODAL_VIEW;
	}

	@RequestMapping(value = "/pageView")
	@ResponseBody
	public Map<String, Object> pageView(EmailReceiver queryObject, PageTools pageTools,
			@ModelAttribute("user") UserInfo user) {
		logger.debug("pageView request : " +queryObject);
		
		Long size = emailReceiverService.selectPageTotalCount(queryObject);
		pageTools.setTotal(size);
		Map<String,Object> result = new HashMap<String,Object>();
		result.put("pageTools", pageTools);
		return result;
	}
	
	@RequestMapping(value = "/loadPage")
	public ModelAndView loadPage(EmailReceiver queryObject, PageTools pageTools,
			@ModelAttribute("user") UserInfo user) throws Exception {
		logger.debug("loadPage EmailReceiver request:" + queryObject + " page info ===" + pageTools);
		
		queryObject.setPageTools(pageTools);
		ModelAndView mv = new ModelAndView(LOAD_PAGE_VIEW);
		Long size = emailReceiverService.selectPageTotalCount(queryObject);
		pageTools.setTotal(size);
		List<EmailReceiver> ciList = emailReceiverService.selectPageByObjectForList(queryObject);
		logger.debug("loadPage EmailReceiver result list info =====:" + ciList);
		
		mv.addObject("resultList", ciList);
		mv.addObject("pageTools", pageTools);
		mv.addObject("queryObject", queryObject);

		return mv;
	}

	@RequestMapping(value="/emailReceiverEdit")
	@ResponseBody
	public Map<String,Object> edit(EmailReceiver editData){
		logger.info("emailReceiverEdit request:{}",editData);
		int num = emailReceiverService.updateByPrimaryKeySelective(editData);
		
		Map<String,Object> map = new HashMap<String,Object>();
		map.put("success", true);
		map.put("editNumber", num);
		return map;
	}
	
	@RequestMapping(value="/emailReceiverAdd")
	@ResponseBody
	public Map<String,Object> add(EmailReceiver addData) throws Exception{
		logger.info("emailReceiverAdd request:{}",addData);
		addData.setCreatedDate(DateUtils.dateToStrLong(new Date()));
		int num = emailReceiverService.insertSelective(addData);
		
		Map<String,Object> map = new HashMap<String,Object>();
		map.put("success", true);
		map.put("addNumber", num);
		return map;
	}
	
	@RequestMapping(value="/emailReceiverDel")
	@ResponseBody
	public Map<String,Object> emailReceiverDel(Long id) throws Exception{
		logger.info("emailReceiverDel request:{}",id);
		int num = emailReceiverService.deleteByPrimaryKey(id);
		
		Map<String,Object> map = new HashMap<String,Object>();
		map.put("success", true);
		map.put("addNumber", num);
		return map;
	}
}