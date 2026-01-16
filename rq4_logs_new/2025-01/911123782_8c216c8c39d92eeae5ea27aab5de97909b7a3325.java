package com.roomfit.be.user;

import jakarta.transaction.Transactional;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
@RequiredArgsConstructor
public class UserServiceImpl implements UserService{
    private final UserRepository userRepository;

    @Transactional
    @Override
    public UserDTO.Response create(UserDTO.Create request) {
        User newUser = User.createUser(request.getNickname(), request.getEmail(), request.getPassword());
        User savedUser = userRepository.save(newUser);

        return UserDTO.Response.of(savedUser);
    }

    @Override
    public UserDTO.Response readById(Long id) {
        User foundUser =  userRepository.findById(id)
                .orElseThrow(()-> new UserNotFoundException(""));

        return UserDTO.Response.of(foundUser);
    }
    public List<UserDTO.Response> readAll() {
        List<User> foundUser = userRepository.findAll();

        return foundUser.stream()
                .map(UserDTO.Response::of)
                .toList();
    }
}