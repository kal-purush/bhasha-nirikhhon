package com.project.menus.controller;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;

import com.green.menus.service.MenuService;
import com.green.menus.vo.MenuVo;

@Controller
@RequestMapping("/Menus")
public class MenuController {
	
	@Autowired
	private MenuService menuService;
	
	@RequestMapping("/List")    //  "/Menus/List"
	public  String   menuList( Model model ) {
		
		// 메뉴목록 조회
		List<MenuVo> menuList = menuService.getMenuList();
		model.addAttribute("menuList", menuList);
		
		return "menus/list";  // "/WEB-INF/views/menus/list.jps'
		
	}
	
	// /Menus/WriteForm
	@RequestMapping("/WriteForm")
	public String   writeForm() {
		return "menus/write";
	}
	
	// /Menus/Write
	@RequestMapping("/Write")
	public String   write( MenuVo  menuVo  ) {
		
		// 메뉴추가
		menuService.insertMenu( menuVo );
		
		return "redirect:/Menus/List";
	}
	
	// /Menus/WriteForm2
	@RequestMapping("/WriteForm2")
	public String   writeForm2() {
		return "menus/write2";
	}
	
	// /Menus/Write2
	@RequestMapping("/Write2")
	public String   write2( MenuVo  menuVo  ) {
		
		// 메뉴추가
		menuService.insertMenu2( menuVo );
		
		return "redirect:/Menus/List";
	}
	
	// /Menus/MenuDelete/${ menu.menu_id }"
	@RequestMapping("/MenuDelete/{menu_id}")
	public  String   menuDelete(@PathVariable String menu_id) {
		menuService.deleteMenu( menu_id );
		
		return "redirect:/Menus/List";
	}
	
	// /Menus/MenuUpdateForm/${ menu.menu_id }
	@RequestMapping("/MenuUpdateForm/{menu_id}")
	public   String   menuUpdateForm(@PathVariable String menu_id, Model model) {
		
		MenuVo  menuVo = menuService.menuView( menu_id );
		model.addAttribute( "menu", menuVo );
		
		return  "menus/update";		
	}
	
	// /Menus/MenuUpdate
	@RequestMapping("/MenuUpdate")
	public   String   menuUpdate( MenuVo menuVo ) {
		
		menuService.menuUpdate( menuVo );
		
		return "redirect:/Menus/List";
	}
	
	
	
}





