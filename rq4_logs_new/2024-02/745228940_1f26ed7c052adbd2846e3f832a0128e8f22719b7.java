import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

@RestController
@RequestMapping("/api") // Base path for API endpoints
public class Controller {

    @PostMapping("/login")
    @ResponseBody
    public String login(@RequestParam String username, @RequestParam String passoword) {
        String authToken = Authentication.authenticate(username, passoword);
        if(authToken != null) {
            return authToken;
        } else {
            return "Invalid Credentials";
        }
    }

}