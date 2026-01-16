package com.tenco.bank.dto;

import com.tenco.bank.repository.model.Account;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/*
  날짜 : 2025.02.14 (금)
  이름 : 김민희
  내용 :
 */

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class AccountSaveDTO {
    private String number;
    private String password;
    private Long balance; // 입금 금액

    // 세션 정보에 사용자 ID를 받아서 Account 를 생성 해야 한다.
    public Account toAccount(int principalId) {
        return Account.builder()
                .number(this.number)
                .password(this.password)
                .balance(this.balance)
                .userId(principalId)
                .build();
    }
}