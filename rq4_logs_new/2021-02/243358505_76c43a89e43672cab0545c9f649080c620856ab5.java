package mybase.services;

import lombok.extern.slf4j.Slf4j;
import mybase.domain.UserAccount;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.stereotype.Service;

@Slf4j
@Service
public class HttpService {

    public void checkUserAccount(UserAccount userAccount) {
        Authentication authentication = SecurityContextHolder.getContext().getAuthentication();
        log.info("userAccount: " + userAccount);
        log.info("authentication: " + authentication);
    }
}