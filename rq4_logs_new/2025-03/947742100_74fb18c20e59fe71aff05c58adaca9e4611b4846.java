package account;

import lombok.Getter;
import lombok.Setter;

import java.sql.Timestamp;
import java.time.LocalDateTime;

@Getter @Setter
public class Account {
    private long accountId;
    private String accountName;
    private String accountEmail;
    private String accountPassword;
    private Timestamp accountRegisterTime;
    private Timestamp accountModifyTime;

    public Account(String name, String email, String password) {
        this.accountName = name;
        this.accountEmail = email;
        this.accountPassword = password;
        this.accountRegisterTime = Timestamp.valueOf(LocalDateTime.now());
    }

    @Override
    public String toString() {
        return "[" + accountId + "]번 회원\n"
                + "계졍 : " + accountName + "\n"
                + "이메일 : " + accountEmail + "\n"
                + "가입일 : " + accountRegisterTime + "\n";
    }
}