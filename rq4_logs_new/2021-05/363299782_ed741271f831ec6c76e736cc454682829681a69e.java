package com.javatechie.crud.example.Impl;

import com.javatechie.crud.example.dto.response.LoginResponse;
import com.javatechie.crud.example.entity.User;
import com.javatechie.crud.example.exceptions.DataNotFoundException;
import com.javatechie.crud.example.exceptions.UserCredentialsDoNotMatchException;
import com.javatechie.crud.example.repository.LoginRepository;
import com.javatechie.crud.example.service.LoginService;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.modelmapper.ModelMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class LoginServiceImpl implements LoginService {

    private Log logger= LogFactory.getLog(LoginServiceImpl.class);

    private ModelMapper modelMapper= new ModelMapper();

    @Autowired
    private LoginRepository repository;

    public LoginResponse login(String username, String password){

        logger.info("In user service class with valid request object: "+username);
        LoginResponse response = null;
        logger.info("Before going to repository");
        User user = repository.findUserByName(username);

        logger.info("Checking for User: "+username+" ifExists");
        if(user != null) {
            logger.info("User exists. Validating respective password");
            if(password.equals(user.getPassword())) {
                logger.info("User credentials match, mapping to response object");
                response= modelMapper.map(user, LoginResponse.class);
            }
            else {
                logger.error("User: "+ username+" Exists. Password donot match");
                throw new UserCredentialsDoNotMatchException("User "+username+" Found. Password is incorrect");
            }
        }
        else{
            logger.error("No user data with username: "+username+" Found");
            throw new DataNotFoundException("User with "+username+" and password not found");
        }

        return response;
    }
}