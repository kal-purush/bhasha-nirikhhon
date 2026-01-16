import repository.AccountRepository;
import repository.UserRepository;
import view.Menu;


    public class MenuApp {
        public static void main(String[] args) {
            // Создаем репозитории
            UserRepository userRepository = new UserRepository();
            AccountRepository accountRepository = new AccountRepository();
            CurrencyRepository currencyRepository = new CurrencyRepository();
            OperationRepository operationRepository = new OperationRepository();

            // Создаем сервисы
            UserService userService = new UserService(userRepository);
            AccountService accountService = new AccountService(accountRepository);
            CurrencyService currencyService = new CurrencyService(currencyRepository);
            OperationService operationService = new OperationService(operationRepository);

            // Создаем консольное меню
            Menu menu = new Menu(userService, accountService, currencyService, operationService);

            // Запускаем консольное меню
            menu.showMenu();
        }
    }

}