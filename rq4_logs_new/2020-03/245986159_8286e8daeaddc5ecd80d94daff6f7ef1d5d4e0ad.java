package lk.sack.livescore.api;

import lk.sack.livescore.beans.TestMatch;
import lk.sack.livescore.exception.UnauthorizedUserException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.web.bind.annotation.*;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpSession;
import java.util.Map;

@RestController
public class MainController {
    private Logger logger = LoggerFactory.getLogger(MainController.class);

    @Autowired
    private SimpMessagingTemplate template;

    @PostMapping("/login")
    public String login(HttpSession session, @RequestBody Map<String, Object> payload) throws UnauthorizedUserException {
        String username = (String) payload.get("username");
        String password = (String) payload.get("password");

        if(username == null || password == null){
            String message = "Username or password is not available";
            logger.warn(message);
            throw new UnauthorizedUserException(message);
        }

        if(username.equals(System.getenv("LIVE_USERNAME")) && password.equals(System.getenv("LIVE_PASSWORD"))){
            session.setAttribute("isAuthorizedUser", true);
            logger.info("User logged in to the platform");
            return "Logged in";
        }else {
            String message = "Invalid credentials";
            logger.warn(message);
            throw new UnauthorizedUserException(message);
        }
    }

    @PostMapping("/logout")
    private void logout(HttpServletRequest request){
        HttpSession session = request.getSession(false);
        if (session != null) {
            session.invalidate();
        }
    }

    @PostMapping("/update-test-match")
    public void updateScore(HttpSession session, @RequestBody TestMatch match) throws UnauthorizedUserException {
        if (session.getAttribute("isAuthorizedUser") != null && session.getAttribute("isAuthorizedUser").equals(true)){
            // Send the payload to the websocket
            this.template.convertAndSend("/topic/test-match", match);
        } else{
            String message = "Unauthorized attempt to update the score";
            logger.warn(message);
            throw new UnauthorizedUserException(message);
        }
    }
}