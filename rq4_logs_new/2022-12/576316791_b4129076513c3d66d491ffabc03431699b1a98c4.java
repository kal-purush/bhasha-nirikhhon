package ie.atu.microservices.Controller;


import ie.atu.microservices.Exceptions.UserNotFoundException;
import ie.atu.microservices.Model.User;
import ie.atu.microservices.Repo.UserRepo;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
public class UserController {

    @Autowired
    private UserRepo repo;

    @PostMapping("/addUser")
    public String addUser(@RequestBody User user)
    {
        if(repo.existsById(user.getId()))
        {
            throw new IllegalStateException("User Id taken!");
        }
        else {
            repo.save(user);
        }
        return "Added Successfully";
    }

    @GetMapping("/allUsers")
    public List<User> getUsers()
    {
        return repo.findAll();
    }

    @GetMapping("/findUser/{id}")
    public User getUser(@PathVariable(value = "id") int id) throws UserNotFoundException
    {
        User user;
        if(repo.existsById(id))
        {
           user = repo.findbyId(id);
        }
        else {
            throw new UserNotFoundException(id);
        }
        return user;
    }

    @DeleteMapping("/delete/{id}")
    public String deleteUser(@PathVariable int id)
    {
        String response;
        if(repo.existsById(id))
        {
            repo.deleteById(id);
            response = "User deleted successfully";
        }
        else
        {
            response = "Failed to delete user";
            throw new UserNotFoundException(id);
        }
        return response;
    }
}