package com.douzone.jblog.controller;

import java.util.List;
import java.util.Optional;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.ui.ModelMap;
import org.springframework.web.bind.annotation.ModelAttribute;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.multipart.MultipartFile;

import com.douzone.jblog.service.BlogService;
import com.douzone.jblog.vo.BlogVo;
import com.douzone.jblog.vo.CartegoryVo;
import com.douzone.jblog.vo.PostVo;
import com.douzone.security.Auth;

@Controller
@RequestMapping("/{id:(?!assets).*}")
public class BlogController {

	@Autowired
	private BlogService blogService;
	
	@Auth
	@RequestMapping( {"", "/{pathNo1}", "/{pathNo1}/{pathNo2}" } )
	public String mian(@PathVariable("id") String id,
					   @PathVariable Optional<Long> pathNo1,
					   @PathVariable Optional<Long> pathNo2,
					   ModelMap modelMap) {

		Long cartegoryno = 0L;
		Long postNo = 0L;
		
		if (pathNo2.isPresent()) {
			postNo = pathNo2.get();
			cartegoryno = pathNo1.get();
		} else if (pathNo1.isPresent()) {
			cartegoryno = pathNo1.get();
			postNo = blogService.getpostno(cartegoryno);
		} else {
			cartegoryno = blogService.getcartegoryno(id);
			postNo = blogService.getpostno(cartegoryno);
			
		}
		modelMap.putAll(blogService.getAll(id, cartegoryno, postNo));
		return "blog/blog-main";
	}
	@Auth
	@RequestMapping(value = "admin/basic", method=RequestMethod.GET)
	public String blogbasic(@PathVariable("id")Optional<String> id, Model model) {
		BlogVo blogVo = blogService.getList(id.get());
		model.addAttribute("blogVo", blogVo);
		return "blog/blog-admin-basic";
	}
	@Auth
	@RequestMapping(value = "admin/basic", method=RequestMethod.POST)
	public String blogbasic(@PathVariable("id")String id,
							@RequestParam(value="file") MultipartFile multipartFile,
							@ModelAttribute BlogVo blogVo) {
		String logo = blogService.restore(multipartFile);
		blogVo.setLogo(logo);
		blogVo.setId(id);
		blogService.updateblog(blogVo);
		
		return "blog/blog-admin-category";
	}
	@Auth
	@RequestMapping("/admin/cartegory")
	public String blogcategory(@PathVariable("id")Optional<String> id,
							   Model model) {
		
		List<CartegoryVo> list = blogService.cartegoryList(id.get());
		model.addAttribute("list", list);
		BlogVo blogVo = blogService.getList(id.get());
		model.addAttribute("blogVo", blogVo);
		
		return "blog/blog-admin-category";
	}
	@Auth
	@RequestMapping(value="/admin/cartegory", method=RequestMethod.POST)
	public String blogcategory(@PathVariable("id")String id,
							   @ModelAttribute CartegoryVo cartegoryVo) {
		cartegoryVo.setId(id);
		blogService.cartegoryaddinsert(cartegoryVo);
		
		return "redirect:/"+ id;
	}
	@Auth
	@RequestMapping("/delete/{no}")
	public String delete(@PathVariable("id")String id,
						 @PathVariable("no")Long no){
		blogService.deleteCartegory(no);
		return "redirect:/"+ id +"/blog-admin-category";
	}
	@Auth
	@RequestMapping(value="/admin/post", method=RequestMethod.GET)
	public String blogwrite(@PathVariable("id")Optional<String> id,
			   				Model model) {
		
		List<CartegoryVo> list = blogService.cartegoryList(id.get());
		model.addAttribute("list", list);
		
		BlogVo blogVo = blogService.getList(id.get());
		model.addAttribute("blogVo", blogVo);
		
		return "blog/blog-admin-write";
	}
	@Auth
	@RequestMapping(value="/admin/post", method = RequestMethod.POST)
	public String blogwrite(@PathVariable("id")String id,
							@ModelAttribute PostVo postVo, CartegoryVo cartegoryVo) {
		blogService.postinsert(postVo);
		
		return "redirect:/"+ id;
	}
}