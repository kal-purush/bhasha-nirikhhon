package Controller;

import Services.UserService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.*;


@Controller
@RequestMapping("/user")
public class UserWeb {
    @Autowired
    private UserService userService;

    @RequestMapping(value="/checkUser")
    @ResponseBody
    public int checkUser(@RequestParam String username,@RequestParam String password){
        return userService.checkUser(username,password);
    }

    @RequestMapping(value="addUser")
    @ResponseBody
  public int addUser(@RequestParam String username,@RequestParam String password,@RequestParam String email,@RequestParam String phone){
      return userService.addUser(username,password,email,phone);
    }
}