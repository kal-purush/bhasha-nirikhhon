package com.mega.biz.join.model.dto;

import com.mega.biz.join.model.domain.Email;
import com.mega.biz.join.model.domain.Password;
import com.mega.biz.join.model.domain.Phone;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class UserValidationDTO {

    @Getter(AccessLevel.NONE)
    @Setter(AccessLevel.NONE)
    private Email email;
    @Getter(AccessLevel.NONE)
    @Setter(AccessLevel.NONE)
    private Password password;
    private String name;
    @Getter(AccessLevel.NONE)
    @Setter(AccessLevel.NONE)
    private Phone phone;
    private String encrypedPassword;
    private Long userStatus;
    private String salt;

    public void setEmail(String email) {
        this.email = new Email(email);
    }

    public void setPassword(String password) {
        this.password = new Password(password);
    }

    public void setPhone(String phone) {
        this.phone = new Phone(phone);
    }

    public String getEmail() {
        return email.getEmail();
    }

    public String getPassword() {
        return password.getPassword();
    }

    public String getPhone() {
        return phone.getPhone();
    }
}