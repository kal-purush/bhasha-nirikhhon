package vask.pet.swapme.userservice.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.security.crypto.bcrypt.BCryptPasswordEncoder;
import org.springframework.stereotype.Service;
import vask.pet.swapme.userservice.UserRepository;
import vask.pet.swapme.userservice.dto.UserDto;
import vask.pet.swapme.userservice.exeption.UserNotSaveException;
import vask.pet.swapme.userservice.http.requests.CreateUserRequest;
import vask.pet.swapme.userservice.model.User;
import vask.pet.swapme.userservice.util.UserMapper;

import javax.transaction.Transactional;
import javax.ws.rs.core.Response;

@Service
@RequiredArgsConstructor
@Slf4j
public class UserService {
    private final BCryptPasswordEncoder passwordEncoder;
    private final UserRepository userRepository;
    private final KeycloakAdminClientService keycloakService;
    private final UserMapper userMapper;




    @Transactional
    public Response saveUser(CreateUserRequest createUserRequest){
        Response response = keycloakService.createKeycloakUser(createUserRequest);

        if (response.getStatus() == 201) {
            User user = userMapper.toUserFromCreateUserRequest(createUserRequest);
            String userId = response.getLocation().getPath().replaceAll(".*/([^/]+)$", "$1");

//            user.setUserId((long) Integer.parseInt(userId));
            log.info("user with username: {} was accepted by keycloak and saved in database with id: {}",user.getUsername(),user.getUserId());
//            return userMapper.toUserDtoFromCreateUserRequest(userRepository.save(user));
            userRepository.save(user);
            return response;

        }
        log.info("user with username: {} didn't accept by keycloak",createUserRequest.getUsername());
        throw new UserNotSaveException(String.format("user with username: {} didn't accept by keycloak",createUserRequest.getUsername()));
    }




}