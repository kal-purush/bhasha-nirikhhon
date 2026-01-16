package com.admin.manager.controller;

import javax.servlet.http.Cookie;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;

import com.admin.common.pojo.ResultData;
import com.admin.manager.pojo.Content;
import com.admin.manager.pojo.User;
import com.admin.manager.service.ArticleService;
import com.admin.manager.service.UserLoginService;

@Controller
@RequestMapping("/blog_front_api")
public class ArticleController {
	
	@Value("${DJ_TOKEN_KEY}")
	private String DJ_TOKEN_KEY;
	
	@Autowired
	private ArticleService articleService;
	
	@Autowired
	private UserLoginService loginService;
	
	
	@RequestMapping(value="/article/getArticleCategory.do",method=RequestMethod.POST)
	@ResponseBody
	public ResultData getArticleCategory() {
		ResultData result = articleService.getArticleCategory();
		return result;
	}
	
	@RequestMapping(value="/article/addAnArticle.do",method=RequestMethod.POST)
	@ResponseBody
	public ResultData addAnArticle(HttpServletRequest request, HttpServletResponse response, Content content) {
		
		Cookie[] arr = request.getCookies();
    	String token = "";
    	if (arr != null) {
    		for(Cookie s: arr){
				if (s.getName().equals(DJ_TOKEN_KEY)) {
					token = s.getValue();
					break;
				}
			    
			}
    	}
    	User loginUser = (User) loginService.getUserByToken(token).getData();
    	content.setAuthor(loginUser.getUsername());
		
		ResultData result = articleService.addAnArticle(content);
		return result;
	}
	
	
	@RequestMapping(value="/article/getAllArticle.do",method=RequestMethod.POST)
	@ResponseBody
	public ResultData getAllArticle() {
		ResultData result = articleService.getAllArticle();
		return result;
	}
	
	@RequestMapping(value="/article/getAnArticle.do",method=RequestMethod.POST)
	@ResponseBody
	public ResultData getAnArticle(Long id) {
		ResultData result = articleService.getAnArticle(id);
		return result;
	}
	
}