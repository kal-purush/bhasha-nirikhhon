package ex1.mb.controller;
import ex1.mb.entity.User;
import ex1.mb.service.UserService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@Controller
@RequestMapping("/user")
public class UserController {

    @Autowired
    private UserService userService;

    @ResponseBody
    @RequestMapping(value = "/add", method = RequestMethod.POST, produces = "application/json;charset=utf-8")
    public User addUser(@RequestBody User user) {
        userService.addUser(user);
        return user;
    }

    @ResponseBody
    @RequestMapping(value = "/get/{id}", method = RequestMethod.GET, produces = "application/json;charset=utf-8")
    public User getUserById(@PathVariable(value = "id") long id) {
        return userService.getUserById(id);
    }

    @ResponseBody
    @RequestMapping(value = "/get/all", method = RequestMethod.GET, produces = "application/json;charset=utf-8")
    public List<User> getAllUsers() {
        return userService.getAllUsers();
    }

    @ResponseBody
    @RequestMapping(value = "/update", method = RequestMethod.PUT, produces = "application/json;charset=utf-8")
    public User updateUser(@RequestBody User user) {
        return userService.updateUser(user);
    }

    @ResponseBody
    @RequestMapping(value = "/delete/{id}", method = RequestMethod.DELETE, produces = "application/json;charset=utf-8")
    public User deleteUser(@PathVariable(value = "id") long id) {
        return userService.deleteUser(id);
    }
}
