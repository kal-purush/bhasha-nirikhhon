package hr.tvz.keepthechange.controller;

import hr.tvz.keepthechange.dto.TransactionDto;
import hr.tvz.keepthechange.enumeration.TransactionCategory;
import hr.tvz.keepthechange.enumeration.TransactionType;
import hr.tvz.keepthechange.service.TransactionService;
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

/**
 * Contains mappings related to transactions.
 */
@Controller
public class TransactionController {

	private static final Logger LOGGER = LoggerFactory.getLogger(RegistrationController.class);
	private final TransactionService transactionService;
	private final WalletService walletService;

	public TransactionController(TransactionService transactionService, WalletService walletService) {
		this.transactionService = transactionService;
		this.walletService = walletService;
	}


	/**
	 * Creates new transaction if there are no validation errors
	 *
	 * @param transactionDto transaction form data
	 * @param errors validation errors
	 * @param model model
	 * @return transaction info view
	 */
	@PostMapping("transaction/new")
	public String createTransaction(@Validated TransactionDto transactionDto, Errors errors, Model model) {
		if(errors.hasErrors()) {
			model.addAttribute("transactionDto", transactionDto);
			model.addAttribute("type", TransactionType.values());
			model.addAttribute("category", TransactionCategory.values());
			LOGGER.info("Form was completed with errors. Redirecting back to form");
			return "transaction";
		} else {

			model.addAttribute("transaction", transactionService.saveTransaction(transactionDto));
			model.addAttribute("wallet", walletService.calculateWallet());
			LOGGER.info("Transaction " + transactionDto.getName() + " saved to database");
			return "transactionInfo";
		}
	}

	/**
	 * Initializes from for new transaction.
	 * @param model model
	 * @return transaction view
	 */
	@GetMapping("transaction/new")
	public String showTransaction(Model model) {
		model.addAttribute("transactionDto", new TransactionDto());
		model.addAttribute("type", TransactionType.values());
		model.addAttribute("category", TransactionCategory.values());
		return "transaction";
	}

	/**
	 * Gets transaction by Id and populated form with data. Used for transaction update.
	 *
	 * @param model model
	 * @param id transaction id
	 * @return transaction view
	 */
	@GetMapping("transaction/{id}")
	public String editTransaction(Model model, @PathVariable("id") Long id) {

		model.addAttribute("transactionDto", transactionService.getTransactionsById(id));
		model.addAttribute("category", TransactionCategory.values());
		model.addAttribute("type", TransactionType.values());
		return "transaction";
	}

	/**
	 * Used for deleting transactions by id.
	 *
	 * @param model model
	 * @param id transaction id
	 * @return redirect to index
	 */
	@GetMapping("transaction/delete/{id}")
	public String deleteTransaction(Model model, @PathVariable("id") Long id) {
		transactionService.deleteTransactionById(id);
		LOGGER.info("Transaction with id" + id + " deleted from database");
		return "redirect:/index";
	}
}