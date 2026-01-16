
package it.project1.account;

import jakarta.annotation.Nonnull;

public class AccountDTO {
    @Nonnull
    private int id;
    @Nonnull
    private String nickName;
    @Nonnull
    private String password;

    public AccountDTO() {
    }

    public AccountDTO(int id, String nickName, String password) {
        this.id = id;
        this.nickName = nickName;
        this.password = password;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getNickName() {
        return nickName;
    }

    public void setNickName(String nickName) {
        this.nickName = nickName;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }
}