package cn.dface.hightlight_springmvc4;

import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.ModelAttribute;
import org.springframework.web.bind.annotation.RequestMapping;

@Controller
public class AdviceController {
	
	@RequestMapping("/advice")
	public String getSomething(@ModelAttribute("user")String msg){
		
		throw new IllegalArgumentException("非常抱歉参数有误，来自"+msg);
	}
}