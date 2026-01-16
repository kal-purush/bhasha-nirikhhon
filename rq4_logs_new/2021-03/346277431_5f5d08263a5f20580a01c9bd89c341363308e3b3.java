package hr.tvz.keepthechange.controller;

import hr.tvz.keepthechange.dto.ExpenseDto;
import hr.tvz.keepthechange.enumeration.ExpenseCategory;
import hr.tvz.keepthechange.service.ExpenseService;
import hr.tvz.keepthechange.service.WalletService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.validation.Errors;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;

import javax.websocket.server.PathParam;

@Controller
public class ExpenseController {

	private static final Logger LOGGER = LoggerFactory.getLogger(RegistrationController.class);
	public static final String EXPENSE_DTO = "expenseDto";
	private final ExpenseService expenseService;
	private final WalletService walletService;

	public ExpenseController(ExpenseService expenseService, WalletService walletService) {
		this.expenseService = expenseService;
		this.walletService = walletService;
	}

	@PostMapping("expense/new")
	public String createExpense(@Validated ExpenseDto expenseDto, Errors errors, Model model) {
		if(errors.hasErrors()) {
			model.addAttribute("expense", expenseDto);
			model.addAttribute("type", ExpenseCategory.values());
			LOGGER.info("Form was completed with errors. Redirecting back to form");
			return "expense";
		} else {

			model.addAttribute("expense", expenseService.saveExpense(expenseDto));
			model.addAttribute("wallet", walletService.calculateWallet());
			LOGGER.info("Expense " + expenseDto.getName() + " saved to database");
			return "expenseInfo";
		}
	}

	@GetMapping("expense/new")
	public String showExpense(Model model) {
		model.addAttribute("expense", new ExpenseDto());
		model.addAttribute("type", ExpenseCategory.values());
		return "expense";
	}

	@GetMapping("expense/{id}")
	public String editExpense(Model model, @PathVariable("id") Long id) {
		model.addAttribute("expense", expenseService.getExpensesById(id));
		model.addAttribute("type", ExpenseCategory.values());
		return "expense";
	}

}