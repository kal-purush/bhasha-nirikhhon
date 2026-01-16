package fr.esir.jxc.services;

import fr.esir.jxc.DTO.UserCreationDTO;
import fr.esir.jxc.utils.EmailFormatChecker;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.StringUtils;

import java.security.InvalidParameterException;

@Slf4j
public class UserCreationService {
    public static void validateUserCreationRequest(UserCreationDTO user) {
        if (StringUtils.isBlank(user.getUsername() )
                || !EmailFormatChecker.isEmailValid(user.getEmail())
                || user.getPassword() == null
                || user.getPassword().length() < 8)
        {
            log.warn("Invalid user creation request.");
            throw new InvalidParameterException();
        }
    }
}