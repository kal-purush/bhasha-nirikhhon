package com.capstone.dentalclinic.demo.services;

import java.util.List;
import java.util.Optional;

import org.springframework.stereotype.Service;

import com.capstone.dentalclinic.demo.model.UserModel;
import com.capstone.dentalclinic.demo.repository.UserModelRepository;

@Service
public class UserModelServices {
    
    private final UserModelRepository userModelRepository;

    public UserModelServices(UserModelRepository userModelRepository) {
        this.userModelRepository = userModelRepository;
    }

    // Gett all The data in the UserModel database 
    public List<UserModel> getAllUser() {
        return userModelRepository.findAll();
    }

    // get user by Id 
    public UserModel getUserById(Long id) {
        Optional<UserModel> userModelOptional = userModelRepository.findById(id);
        
        if(userModelOptional.isPresent()) {
            return userModelOptional.get();
        }
        return null;
    }

    // This will Create data for the UserModel
    public UserModel createUser(UserModel userModel) {
        
        return userModelRepository.save(userModel);
    }

    // This will Update existing UserData 
    public UserModel updateUser(Long id, UserModel userModel) {
        Optional<UserModel> userModelOptional = userModelRepository.findById(id);
        
        if(userModelOptional.isPresent()) {
            return userModelRepository.save(userModel);         
        }
        return null;
    }

}