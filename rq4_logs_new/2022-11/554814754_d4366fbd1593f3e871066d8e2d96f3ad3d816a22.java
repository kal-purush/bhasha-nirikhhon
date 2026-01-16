package project.TadeM.Transaction.Controller;


import java.util.List;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.util.CollectionUtils;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import project.TadeM.Transaction.Dto.TransactionDto;
import project.TadeM.Transaction.model.TransactionParam;
import project.TadeM.Transaction.service.TransactionService;
import project.TadeM.product.model.ServiceResult;
import project.TadeM.util.PageUtil;

@RequiredArgsConstructor
@Controller
public class TransactionController {


	private final TransactionService transactionService;

	@GetMapping("/transaction/list")
	public String list(Model model, TransactionParam parameter){

		parameter.init();
		List<TransactionDto> productList = transactionService.list(parameter);
		long totalCount = 0;

		if (!CollectionUtils.isEmpty(productList)){
			totalCount = productList.get(0).getTotalCount();
		}

		String queryString = parameter.getQueryString();
		PageUtil pageUtil = new PageUtil(totalCount, parameter.getPageSize(),
			parameter.getPageIndex(), queryString);

		model.addAttribute("list", productList);
		model.addAttribute("totalCount", totalCount);
		model.addAttribute("pager", pageUtil.pager());

		return "/transaction/list";
	}

	@GetMapping("/transaction/list_Buyer")
	public String listBuyer(Model model, TransactionParam parameter){

		parameter.init();
		List<TransactionDto> productList = transactionService.list(parameter);
		long totalCount = 0;

		if (!CollectionUtils.isEmpty(productList)){
			totalCount = productList.get(0).getTotalCount();
		}

		String queryString = parameter.getQueryString();
		PageUtil pageUtil = new PageUtil(totalCount, parameter.getPageSize(),
			parameter.getPageIndex(), queryString);

		model.addAttribute("list", productList);
		model.addAttribute("totalCount", totalCount);
		model.addAttribute("pager", pageUtil.pager());

		return "/transaction/list_Buyer";
	}
	@PostMapping("/transaction/status")
	public String status(Model model, TransactionParam parameter){

		parameter.init();
		ServiceResult result = transactionService.updateStatus(parameter.getId(), parameter.getStatus());

		if (!result.isResult()){
			model.addAttribute("message", result.getMessage());
			return "product/error";
		}

		return "redirect:/transaction/list_Buyer";
	}
	}
