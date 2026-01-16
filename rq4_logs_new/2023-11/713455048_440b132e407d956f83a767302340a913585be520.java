package com.uttam.project.mapper;

import java.util.ArrayList;
import java.util.List;

import org.springframework.stereotype.Component;

import com.uttam.project.DTO.Account;
import com.uttam.project.DTO.User;
import com.uttam.project.model.AccountDO;
import com.uttam.project.model.UserDO;

@Component
public class UserMapper {

	
	public List<User> userDOtoDTO(List<UserDO> userDOList) {
		List<User> userDTOList = new ArrayList<>();
		
		for(UserDO userDO : userDOList) {
			User user = User.builder()
					.userId(userDO.getUserId())
					.address(userDO.getAddress())
					.age(userDO.getAge())
					.email(userDO.getEmail())
					.firstName(userDO.getFirstName())
					.lastName(userDO.getLastName())
					.build();
			List<AccountDO> accountDOs = userDO.getAccounts();
			List<Account> accountList =  new ArrayList<>();
			for(AccountDO accountDO : accountDOs) {
				Account account = Account.builder()
						.accountId(accountDO.getAccountId())
						.userName(accountDO.getUserName())
						.password(accountDO.getPassword())
						.build();
				accountList.add(account);				
			}
			user.setAccounts(accountList);
			userDTOList.add(user);
		}
		return userDTOList;
		
	}
	
	public List<UserDO> userDTOtoDO(List<User> userList) {
		List<UserDO> userDOList = new ArrayList<>();
		
		for(User user : userList) {
			UserDO userDO = UserDO.builder()
					.firstName(user.getFirstName())
					.lastName(user.getLastName())
					.address(user.getAddress())
					.age(user.getAge())
					.email(user.getEmail())
					.build();
			List<Account> accounts = user.getAccounts();
			List<AccountDO> accountDOList =  new ArrayList<>();
			for(Account account : accounts) {
				AccountDO accountDO = AccountDO.builder()
						.userName(account.getUserName())
						.password(account.getPassword())
						.build();
				accountDOList.add(accountDO);				
			}
			userDO.setAccounts(accountDOList);
			userDOList.add(userDO);
		}
		return userDOList;	
	}
}