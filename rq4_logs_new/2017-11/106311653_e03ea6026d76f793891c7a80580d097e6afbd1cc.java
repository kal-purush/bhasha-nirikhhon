package ru.clean.process.service.user;

import org.springframework.stereotype.Service;
import ru.clean.process.api.dto.user.User;
import ru.clean.process.api.exceptions.UserServiceException;
import ru.clean.process.api.service.UserService;
import ru.clean.process.api.service.repo_adapters.UserRepositoryAdapter;
import ru.clean.process.service.verification.UserVerifier;
import ru.clean.process.service.verification.VerificationResult;

import java.util.List;
import java.util.stream.Collectors;

/**
 * User service implementation
 */
@Service
public class UserServiceImpl implements UserService {

    private final UserRepositoryAdapter userRepository;
    private final UserVerifier userVerifier;

    public UserServiceImpl(UserRepositoryAdapter userRepository, UserVerifier userVerifier) {
        this.userRepository = userRepository;
        this.userVerifier = userVerifier;
    }

    @Override
    public List<User> getAllUser() {
        return userRepository.getAllUser();
    }

    @Override
    public User saveUser(User user) throws UserServiceException {
        if (user.getId() == null) {
            return createUser(user);
        }
        return updateUser(user);
    }

    @Override
    public User getUserByLogin(String login) throws UserServiceException {
        User result = userRepository.getUserByLogin(login);
        if (result == null) {
            throw new UserServiceException(String.format("User with name %s was not found", login));
        }
        return result;
    }

    /**
     * Update existed user
     *
     * UserServiceException if user DTO hasn't pass verification
     */
    private User updateUser(User user) throws UserServiceException {
        List<VerificationResult> verificationResult = userVerifier.verifyUserOnUpdate(user);
        if (verificationResult.isEmpty()) {
            return userRepository.saveUser(user);
        } else {
            throw new UserServiceException(String.format("Unable to update user because: \n%s",
                    verificationResult.stream().map(VerificationResult::getErrorMessage).collect(Collectors.joining("\n"))));
        }
    }

    /**
     * Create new user
     *
     * @throws UserServiceException if user DTO hasn't pass verification
     */
    private User createUser(User user) throws UserServiceException {
        List<VerificationResult> verificationResult = userVerifier.verifyUserOnCreate(user);
        if (verificationResult.isEmpty()) {
            return userRepository.saveUser(user);
        } else {
            throw new UserServiceException(String.format("Unable to create user because: \n%s",
                    verificationResult.stream().map(VerificationResult::getErrorMessage).collect(Collectors.joining("\n"))));
        }
    }
}