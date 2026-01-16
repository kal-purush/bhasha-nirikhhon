package skillfactory.DreamTeam.globus.it.controllers;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import skillfactory.DreamTeam.globus.it.services.BankService;
import skillfactory.DreamTeam.globus.it.services.BankAccountService;
import skillfactory.DreamTeam.globus.it.dao.entities.bank.BankEntity;
import skillfactory.DreamTeam.globus.it.dto.bank.BankCreationRequests;
import skillfactory.DreamTeam.globus.it.dao.entities.bank.BankAccountEntity;
import lombok.RequiredArgsConstructor;
import java.time.LocalDateTime;

@RestController
@RequiredArgsConstructor
@RequestMapping("/")  // TODO: Base URL for the controller
public class BankController {
    private final BankService bankService;
    private final BankAccountService bankAccountService;
    //private final OperationService operationService;
    
    public BankEntity createBank(BankCreationRequests.CreateBank request) {
        return bankService.createBank(request);
    }

    public BankAccountEntity createBankAccount(BankCreationRequests.CreateBankAccount request) {
        return bankAccountService.createAccount(request);
    }

    public BankAccountEntity getAccountByHolderId(Long id) {
        return bankAccountService.getAccountByHolderId(id);
    }

    public BankAccountEntity getByRecepientDate(LocalDateTime date) {
        return bankAccountService.getByRecepientDate(date);
    }
}