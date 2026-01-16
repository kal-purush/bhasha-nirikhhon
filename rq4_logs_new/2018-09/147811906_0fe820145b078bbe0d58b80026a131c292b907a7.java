import model.Account;
import model.Transaction;
import model.TransactionType;
import model.User;
import service.AccountService;
import service.BankingService;
import service.TransactionHistoryPrintService;
import service.TransactionHistoryService;

import java.math.BigDecimal;
import java.util.List;

public class ApplicationCLI {

    private static AccountService accountService;
    private static TransactionHistoryService transactionHistoryService;
    private static BankingService bankingService;
    private static TransactionHistoryPrintService transactionHistoryPrintService;

    public static void main(String[] args) {
        init();
        User user1 = new User("Kovacs Andras");
        User user2 = new User("Szabo Gabor");
        Account account1 = new Account(user1);
        Account account2 = new Account(user2);
        bankingService.deposit(account1.getId(), new BigDecimal(5000));
        bankingService.deposit(account2.getId(), new BigDecimal(10000));
        bankingService.transfer(account2.getId(), account1.getId(), new BigDecimal(3000));
        List<Transaction> transactions = transactionHistoryService.getTransactionsByAccountIdFilterByType(account1.getId(),TransactionType.DEPOSIT);
        transactionHistoryPrintService.printTransactionHistoryBy(transactions);
    }

    public static void init() {
        accountService = new AccountService();
        transactionHistoryService = new TransactionHistoryService();
        bankingService = new BankingService(transactionHistoryService, accountService);
        transactionHistoryPrintService = new TransactionHistoryPrintService(transactionHistoryService);
    }
}