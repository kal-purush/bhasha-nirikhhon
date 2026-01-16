package org.api.account.dto;

import lombok.Getter;
import lombok.Setter;
import org.api.account.domain.Account;

@Getter
@Setter
public class AccountDto {
    private String email;
    private String nickname;
    private String password;

    public Account toEntity() {
        Account account = new Account();
        account.setEmail(this.email);
        account.setNickname(this.nickname);
        account.setPassword(this.password);

        return account;
    }
}