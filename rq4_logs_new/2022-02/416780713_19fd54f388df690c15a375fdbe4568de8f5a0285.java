package com.ostapdev.todo;

import com.ostapdev.todo.controller.AccountController;
import com.ostapdev.todo.dto.account.CreateAccountDtoRequest;
import com.ostapdev.todo.model.Role;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import static org.junit.jupiter.api.Assertions.assertNotEquals;

@SpringBootTest
public class AccountControllerTest {
    @Autowired
    private AccountController accountController;

    @Test
    public void GetAccount_AccountNameUser22_Return200()  {
        accountController.deleteAccount("user22");
        accountController.addAccount(new CreateAccountDtoRequest("user22","pass",Role.USER));
        assertNotEquals(accountController.getAccountByUsername("user22"),null);
    }
}