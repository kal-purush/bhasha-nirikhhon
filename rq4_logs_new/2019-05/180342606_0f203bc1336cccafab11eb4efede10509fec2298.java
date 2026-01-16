package uk.gov.cshr.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.context.ApplicationListener;
import org.springframework.security.authentication.event.InteractiveAuthenticationSuccessEvent;
import org.springframework.stereotype.Component;
import uk.gov.cshr.exceptions.ForbiddenException;

import java.util.ArrayList;
import java.util.LinkedHashMap;

@Component
public class AuthenticationConfig implements ApplicationListener<InteractiveAuthenticationSuccessEvent> {

    public void onApplicationEvent(InteractiveAuthenticationSuccessEvent e) {
        Object principal = e.getAuthentication();
        ObjectMapper oMapper = new ObjectMapper();
        LinkedHashMap<String, LinkedHashMap<String, LinkedHashMap<String, ArrayList<String>>>> o = oMapper.convertValue(principal, LinkedHashMap.class);
        LinkedHashMap<String, LinkedHashMap<String, ArrayList<String>>> userAuthentication = o.get("userAuthentication");
        ArrayList<String> roles = userAuthentication.get("details").get("roles");
        if (!roles.contains("IDENTITY_MANAGER")) {
            throw new ForbiddenException();
        }
    }
}