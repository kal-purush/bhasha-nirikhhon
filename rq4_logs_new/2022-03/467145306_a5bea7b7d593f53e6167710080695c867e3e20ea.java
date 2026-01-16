package com.company.getyourgoal.controller;

import com.company.getyourgoal.model.User;
import com.company.getyourgoal.repository.GoalRepository;
import com.company.getyourgoal.repository.UserRepository;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.util.List;
import java.util.Optional;

import static org.junit.Assert.*;

@RunWith(SpringJUnit4ClassRunner.class)
@SpringBootTest
public class UserControllerTest {
    @Autowired
    UserRepository userRepository;
    @Autowired
    GoalRepository goalRepository;


    @Before
    public void setUp() throws Exception {
        userRepository.deleteAll();
        goalRepository.deleteAll();
    }

    @Test
    public void addGetDeleteUser() {
        User user = new User();
        user.setFirstName("John");
        user.setLastName("Darren");
        user.setUsername("Jdarren");
        user.setPassword("password");
        user.setEmail("darren@gmail.com");

        user =userRepository.save(user);

        Optional<User> user1 = userRepository.findById(user.getId());

        assertEquals(user1.get(), user);

        userRepository.deleteById(user.getId());

        user1 = userRepository.findById(user.getId());

        assertFalse(user1.isPresent());
    }

    @Test
    public void updateUser() {
        User user = new User();
        user.setFirstName("John");
        user.setLastName("Darren");
        user.setUsername("Jdarren");
        user.setPassword("password");
        user.setEmail("darren@gmail.com");

        user =userRepository.save(user);

        user.setFirstName("John updated");
        user.setLastName("Darren updated");
        user.setUsername("Jdarren");
        user.setPassword("password");
        user.setEmail("darren@gmail.com");

        userRepository.save(user);
        Optional<User> user1 = userRepository.findById(user.getId());
        assertEquals(user1.get(), user);

    }

    @Test
    public void getAllUser() {

        User user = new User();
        user.setFirstName("John");
        user.setLastName("Darren");
        user.setUsername("Jdarren");
        user.setPassword("password");
        user.setEmail("darren@gmail.com");

        user = userRepository.save(user);

        user = new User();
        user.setFirstName("Sarah");
        user.setLastName("Jim");
        user.setUsername("Jsarah");
        user.setPassword("password");
        user.setEmail("sarah@gmail.com");

        user = userRepository.save(user);

        List<User> userList = userRepository.findAll();
        assertEquals(userList.size(), 2);

    }

    @Test
    public void getOneUserById() {

        User user = new User();
        user.setFirstName("John");
        user.setLastName("Darren");
        user.setUsername("Jdarren");
        user.setPassword("password");
        user.setEmail("darren@gmail.com");

        user =userRepository.save(user);

        Optional<User> user1 = userRepository.findById(user.getId());
        assertEquals(user1.get(), user);
        user1 = userRepository.findById(user.getId());
        assertTrue(user1.isPresent());

    }
}