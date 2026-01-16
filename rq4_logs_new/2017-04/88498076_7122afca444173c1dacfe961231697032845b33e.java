package cn.blog.controller;

import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;

/**
* @Coder XIAOYAO
* @Time 2016年4月22日 下午10:58:41
* 列表处理
*/
@Controller
@RequestMapping("/columns")
public class ColumnController {
	
	/**
	 * 跳转到blog首页
	 * @return
	 */
	@RequestMapping("/bloghome")
	public String blogHome() {
		return "columns/bloghome";
	}
	
}