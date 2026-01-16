package com.self.egoboard.domain.user;

import com.self.egoboard.domain.user.dao.UserRepository;
import com.self.egoboard.domain.user.entity.User;
import com.self.egoboard.domain.user.entity.embedded.Address;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class UserTestHelper {

  @Autowired
  UserRepository userRepository;

  private User generate() {
    return this.builder().build();
  }

  private UserBuilder builder() {
    return new UserBuilder();
  }

  public final class UserBuilder {

    private String emailAddress;
    private String password;
    private String username;
    private String nickname;
    private Address address;

    public void setEmailAddress(String emailAddress) {
      this.emailAddress = emailAddress;
    }

    public void setPassword(String password) {
      this.password = password;
    }

    public void setUsername(String username) {
      this.username = username;
    }

    public void setNickname(String nickname) {
      this.nickname = nickname;
    }

    public void setAddress(Address address) {
      this.address = address;
    }

    public User build() {
      return userRepository.save(User.builder()
          .address(address != null ? address : new Address("도시", "거리", "집코드"))
          .emailAddress(emailAddress != null ? emailAddress : "email@gmail.com")
          .password(password != null ? password : "password123")
          .username(username != null ? username : "유저이름")
          .nickname(nickname != null ? nickname : "닉네임")
          .build());
    }
  }
}