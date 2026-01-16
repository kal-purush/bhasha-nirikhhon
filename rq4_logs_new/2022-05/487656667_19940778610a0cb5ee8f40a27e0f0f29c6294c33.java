package com.group7.mezat.Services;

import com.group7.mezat.responses.UserResponse;
import com.group7.mezat.documents.User;
import lombok.AllArgsConstructor;
import org.springframework.stereotype.Service;
import com.group7.mezat.repos.UserRepository;
import com.group7.mezat.requests.UserPasswordUpdateRequest;

import java.util.List;
import java.util.Optional;

@Service
@AllArgsConstructor
public class UserService {

    private UserRepository userRepository;

    public List<User> getUsers() {
        return userRepository.findAll();
    }


    public UserResponse getOneUser(String id) {
        User user = userRepository.findById(id).orElse(null);
        return new UserResponse(user);
    }


    public void deleteUser(String id) {
        userRepository.deleteById(id);
    }


    public User updateUserPassword(String id, UserPasswordUpdateRequest newUser) {
        Optional<User> user = userRepository.findById(id);
        if (user.isPresent()){
            User foundUser = user.get();
//            foundUser.setEmail(newUser.getEmail());
            foundUser.setPassword(newUser.getPassword());
//            foundUser.setPassword(passwordEncoder.encode(newUser.getPassword()));
            return userRepository.save(foundUser);
        }else{
            return null;
        }
    }

    public void saveUser(User user) {
        userRepository.insert(user);
    }

    public UserResponse getOneUserByName(String userName) {
        User user = userRepository.findByuserName(userName);
        return new UserResponse(user);
    }
}