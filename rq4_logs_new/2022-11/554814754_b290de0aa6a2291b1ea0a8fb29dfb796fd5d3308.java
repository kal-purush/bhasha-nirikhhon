package project.TadeM.Admin.Controller;

import java.util.List;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import project.TadeM.Admin.Dto.CategoryDto;
import project.TadeM.Admin.Service.CategoryService;
import project.TadeM.Admin.model.CategoryInput;

@RequestMapping("/admin/category")
@Controller
@RequiredArgsConstructor
public class AdminCategoryController {

	private final CategoryService categoryService;

	@GetMapping("/category_List")
	public String list(Model model){

		List<CategoryDto> list = categoryService.list();
		model.addAttribute("list", list);

		return "/admin/category/category_List";
	}
	@PostMapping("/add")
	public String add(Model model, CategoryInput parameter){

		boolean result = categoryService.add(parameter.getCategoryName());

		return "redirect:/admin/category/category_List";
	}

	@PostMapping("/delete")
	public String del(Model model, CategoryInput parameter){

		boolean result = categoryService.del(parameter.getId());

		return "redirect:/admin/category/category_List";
	}
	@PostMapping("/update")
	public String update(Model model, CategoryInput parameter){

		boolean result = categoryService.update(parameter);

		return "redirect:/admin/category/category_List";
	}


}