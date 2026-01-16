package com.feedlyclone.mapper;

import com.feedlyclone.domain.entity.Account;
import com.feedlyclone.domain.entity.User;
import com.feedlyclone.dto.AccountDTO;
import com.feedlyclone.dto.UserDTO;
import org.springframework.stereotype.Component;

@Component
public class UserMapper implements FeedCustomMapper<User, UserDTO> {

    @Override
    public void mapAtoB(User user, UserDTO userDTO) {
        if (user != null && userDTO != null){
            userDTO.setId(user.getId());
            userDTO.setPassword(user.getPassword());
            userDTO.setName(user.getName());
            if (user.getAccount() != null){
                AccountDTO accountDTO = new AccountDTO();
                accountDTO.setId(user.getAccount().getId());
                userDTO.setAccount(accountDTO);
            }
        }
    }

    @Override
    public void mapBtoA(UserDTO userDTO, User user) {
        if (user != null && userDTO != null){
            user.setId(userDTO.getId());
            user.setPassword(userDTO.getPassword());
            user.setName(userDTO.getName());
            if (userDTO.getAccount() != null){
                Account account = new Account();
                account.setId(userDTO.getAccount().getId());
                user.setAccount(account);
            }
        }
    }

    @Override
    public UserDTO map(User user) {
        UserDTO userDTO = new UserDTO();
        mapAtoB(user, userDTO);
        return userDTO;
    }

    @Override
    public User mapReverse(UserDTO userDTO) {
        User user = new User();
        mapBtoA(userDTO, user);
        return user;
    }
}