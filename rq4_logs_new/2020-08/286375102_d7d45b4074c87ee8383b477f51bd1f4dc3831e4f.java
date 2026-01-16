package com.yasiru.HelloWorldAPI.Controller;
import com.yasiru.HelloWorldAPI.DAO.User;
import com.yasiru.HelloWorldAPI.Service.UserService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;


@RestController
@RequestMapping("/hlwld/api")
public class UserController {
    @Autowired
    private UserService userService;

    @PostMapping("/saveuser")
    public String userSave(@RequestBody User user){
        String result = userService.saveUser(user);
        return result;
    }

    @GetMapping("/getuser/{nic}")
    public User getUserFromUserNic(@PathVariable("nic") String usernic){
        User usr = new User();
        usr = userService.findUserbyUserNic(usernic);
        return usr;
    }
}