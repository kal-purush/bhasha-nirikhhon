package sysjm3.bulbo.bulbo.controllers;

import java.util.List;
import java.util.UUID;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import sysjm3.bulbo.bulbo.model.User;
import sysjm3.bulbo.bulbo.service.UserService;

@RestController
public class UserController {
    
    @Autowired
    UserService userService;
    
        /**
     * GET mapping for all users in the user table at 
     * the /workspaces URL
     *
     * @return Query result from the GET request in JSON format
     */
    @GetMapping("/users")
    public List<User> allUsers() {
        return userService.findAll();
    }
    
    /**
     * POST mapping for /users URL. Essentially creates a new 
     * User object and inserts it in the table users
     *
     * @param user Request body in the format of JSON 
     * representing the values for the User object
     * @return Query result in the form of JSON format representing the 
     * User that was just created
     */
    @PostMapping("/users")
    public User saveUser (@RequestBody User user) {
        return userService.saveUser(user);
    }

    /**
     * DELETE mapping for /users URL. It should delete the User 
     * given as parameter from the users table
     * 
     * @param id Request body in the format of JSON representing the 
     * values for the User object
     */
    @DeleteMapping("/users")
    public void deleteWorkspaceById(@RequestParam UUID id) {
        if (id != null) {
            userService.deleteUserById(id);
        }
    }

     /**
     * GET mapping for /users URL. Returns the tuple with the ID given 
     * as parameters
     * 
     * @param id ID of the Card you want to view
     * @return Query result in the form of JSON format representing the Card
     * that was fetched if it exists
     */
    @GetMapping("/users")
    public User getWorkspaceById(@RequestParam UUID id) {
        return userService.findOne(id);
    }    
    
}