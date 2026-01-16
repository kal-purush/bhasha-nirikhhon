package studentenrollment.username.controller;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import studentenrollment.username.service.UserNameService;

@RestController
@RequestMapping("/api/v3/studentenrollment")
public class UserNameController {

    @Autowired
    private UserNameService userService;

    @GetMapping("/users/availability")
    public ResponseEntity<String> checkUsernameAvailability(@RequestParam("username") String username) {
        // Check if the username already exists
        boolean isUsernameExists = userService.isUsernameExists(username);

        if (isUsernameExists) {
            return ResponseEntity.status(HttpStatus.BAD_REQUEST).body("Username already exists");
        } else {
            return ResponseEntity.ok("Username is available, you can create");
        }
    }
}
