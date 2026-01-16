package com.zy.profit.gateway.web.broker;

import javax.servlet.http.HttpServletRequest;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.RequestMapping;

import com.zy.broker.service.BrokerExtInfoService;
import com.zy.broker.service.BrokerInfoService;

@Controller
@RequestMapping("bk")
public class BrokerController {

	@Autowired
	private BrokerInfoService brokerInfoService;
	@Autowired
	private BrokerExtInfoService brokerExtInfoService;
	
	@RequestMapping("bk/list")
	public String brokerList(Model model,HttpServletRequest request){
		
		model.addAttribute("brokerExtInfos", null);
		
		return "broker/brokerIndex";
	}
}