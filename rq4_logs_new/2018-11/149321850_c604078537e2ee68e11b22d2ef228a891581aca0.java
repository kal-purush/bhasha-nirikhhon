package hu.uni.miskolc.iit.service.impl;

import hu.uni.miskolc.iit.exceptions.UserAlreadyExist;
import hu.uni.miskolc.iit.exceptions.UserCanNotLogin;
import hu.uni.miskolc.iit.exceptions.UserDoesNotExist;
import hu.uni.miskolc.iit.model.Course;
import hu.uni.miskolc.iit.service.UserService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;


@Service
public class AdministratorServiceImpl implements UserService {

    @Autowired


    /**
     * @param lastName
     * @param firstName
     * @param email
     * @param password
     */
    @Override
    public void userRegistration(String lastName, String firstName, String email, String password) throws UserAlreadyExist {

    }

    /**
     * @param user
     * @param password
     * @return
     */
    @Override
    public boolean logger(String user, String password) throws UserCanNotLogin {
        return false;
    }

    /**
     * @param lastName
     * @param firstName
     * @param email
     * @param password
     */
    @Override
    public void userModification(String lastName, String firstName, String email, String password) throws UserDoesNotExist {

    }

    /**
     * @param user
     */
    @Override
    public void userDelete(String user) throws UserDoesNotExist {

    }



}