package se.kth.iv1201.recruitmentapplicationgroup5.service;

import javax.validation.Valid;

import org.modelmapper.ModelMapper;
import org.modelmapper.config.Configuration;
import org.springframework.stereotype.Service;

import se.kth.iv1201.recruitmentapplicationgroup5.model.Account;
import se.kth.iv1201.recruitmentapplicationgroup5.model.dto.AccountDTO;
import se.kth.iv1201.recruitmentapplicationgroup5.model.dto.RegistrationDetails;

@Service
public class AccountService {

	ModelMapper modelMapper = new ModelMapper();

	public AccountService() {
		modelMapper.getConfiguration().setFieldMatchingEnabled(true)
				.setFieldAccessLevel(Configuration.AccessLevel.PRIVATE);
	}

	public AccountDTO addAccount(@Valid RegistrationDetails registrationDetails) {
		Account newAccount = RegistrationToAccount(registrationDetails);
		// Add Account to Reposetory
		AccountDTO newAccountDTO = AccountToDTO(newAccount);
		return newAccountDTO;
	}

	private Account RegistrationToAccount(final RegistrationDetails details) {
		final Account account = modelMapper.map(details, Account.class);
		return account;
	}

	private AccountDTO AccountToDTO(final Account account) {
		final AccountDTO accountDTO = modelMapper.map(account, AccountDTO.class);
		return accountDTO;
	}

}