package by.ras.repository;

import by.ras.BaseTest;

import by.ras.entity.particular.User;
import lombok.extern.log4j.Log4j;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.PageRequest;
import sun.awt.image.ImageWatched;

import java.lang.reflect.Array;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

import static org.junit.Assert.*;

@Log4j
public class UserRepositoryTest extends BaseTest {

    @Autowired
    UserRepository userRepository;

    @Test
    public void createUserTest(){
        log.info("**************************");
        log.info("**************************");
        //find by ID test
        User user = userRepository.saveAndFlush(new User("Max"));
        User testUser =  userRepository.findOne(user.getId());
        log.info(testUser);
        assertEquals(user, testUser);
        log.info("**************************");
        log.info("**************************");

    }

    @Test
    public void findUserTest(){
        log.info("**************************");
        log.info("**************************");
        //find by ID test
        User user = userRepository.saveAndFlush(new User("Max"));
        User testUser =  userRepository.findOne(user.getId());
        log.info(testUser);
        assertEquals(user, testUser);
        log.info("**************************");
        log.info("**************************");
        //find by login test
        testUser = userRepository.findByLogin(user.getLogin());
        log.info(testUser);
        assertEquals(user, testUser);
        log.info("**************************");
        log.info("**************************");

    }

    @Test
    public void editUserTest(){
        log.info("**************************");
        log.info("**************************");
        User user = userRepository.saveAndFlush(new User("Max"));
        User testUser =  userRepository.findOne(user.getId());
        testUser.setName("Mad");
        log.info("**************************");
        log.info("**************************");
        testUser = userRepository.saveAndFlush(testUser);
        user = userRepository.findByLogin("Max");
        assertEquals(user, testUser);
        log.info("**************************");
        log.info("**************************");

    }

    @Test
    public void getListUsersTest(){
        List<User> usersExpected = new LinkedList<User>();
        for(int i = 0; i < 5; i++){
            User user = userRepository.saveAndFlush(new User("Bob" + (i + 1)));
            usersExpected.add(user);
        }
        log.info("**************************");
        log.info("**************************");
        log.info("**************************");
        usersExpected.forEach(u -> log.info(u.toString()));
        log.info("**************************");
        List<User> users = userRepository.findAll();
        log.info("**************************");
        users.forEach(u -> log.info(u));
        log.info("**************************");
        log.info("**************************");
        log.info("**************************");
        assertEquals(usersExpected, users);
    }

    @Test
    public void getPageUsersTest(){
        //setting parameters
        int MAX_USERS = 20; //even nums only
        int usersPerPage = MAX_USERS/2;

        //setting 20 users and adding to a LIST
        List<User> tmpList = new LinkedList<User>();
        List<User> usersExpected1 = new LinkedList<User>();
        List<User> usersExpected2 = new LinkedList<User>();
        for(int i = 0; i < MAX_USERS; i++){
            User user = userRepository.saveAndFlush(new User("Bob" + (i + 1)));
            usersExpected1.add(user);
        }

        //dividing List to list_1(first_10_users) and list_2(rest_users
        for(int i = 0; i < (MAX_USERS/2); i++){
            tmpList.add(usersExpected1.get((MAX_USERS-1) - i));
            usersExpected1.remove((MAX_USERS-1) - i);
        }
        for(int i = 0; i < tmpList.size(); i++){
            usersExpected2.add(tmpList.get((tmpList.size()-1) - i));
        }
        //checking with eyes
        log.info("**************************");
        usersExpected1.forEach(u -> log.info(u.toString()));
        log.info("**************************");
        log.info("**************************");
        usersExpected2.forEach(u -> log.info(u.toString()));
        log.info("**************************");
        //getting pages [0, usersPerPage] and [1, usersPerPage]
        List<User> users1 = userRepository.findAll(new PageRequest(0, usersPerPage)).getContent();
        List<User> users2 = userRepository.findAll(new PageRequest(1, usersPerPage)).getContent();
        log.info("**************************");
        users1.forEach(u -> log.info(u));
        log.info("**************************");
        users2.forEach(u -> log.info(u));
        log.info("**************************");
        log.info("**************************");

        assertEquals(usersExpected1, users1);
        assertEquals(usersExpected2, users2);
    }
}