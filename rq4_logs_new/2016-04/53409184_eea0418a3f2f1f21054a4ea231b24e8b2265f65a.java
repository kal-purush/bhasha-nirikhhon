package wood.shop.controller;

import java.util.ArrayList;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.servlet.ModelAndView;
import wood.item.svc.ItemService;
import wood.shop.dto.Item;
import wood.shop.dto.ParamTO;

@Controller
public class HomeController {
	@Autowired
	private ItemService itemsvc;

	@RequestMapping("/index.do")
	public ModelAndView home (ParamTO params,ModelAndView mav,Map<String, Object> map){
		String queryId ="item.list";
		params.setStartrow(0);
		params.setEndrow(5);
		ArrayList<Item> itemList =itemsvc.itemList(queryId, params);
		mav.addObject("itemList", itemList);
		mav.setViewName("index");
		return mav;
	}
}