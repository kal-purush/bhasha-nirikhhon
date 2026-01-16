package com.smartverse.churchlitebackend.handlers.verifyurlchurch;

import com.smartverse.churchlitebackend.config.security.repository.AuthenticationRepository;
import com.smartverse.churchlitebackend.repository.userConfirmation.UserConfirmationCustomRepository;
import com.smartverse.churchlitebackend_gen.VerifyURL;
import com.smartverse.churchlitebackend_gen.VerifyURLOutput;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class VerifyUrlImpl implements VerifyURL {

    @Autowired
    UserConfirmationCustomRepository userConfirmationRepository;

    @Autowired
    AuthenticationRepository authenticationRepository;

    @Override
    public VerifyURLOutput verifyURL(String token) {

        var userConfirmation = userConfirmationRepository.findByHash(token).orElse(null);
        var output = new VerifyURLOutput();
        output.authorize = false;
        if(userConfirmation != null) {
            output.authorize = true;

            var user = authenticationRepository.findById(userConfirmation.getUserId()).orElse(null);
            if(user != null) {
                user.setUserConfirm(true);
                user.setActive(true);
                authenticationRepository.save(user);
            }
        }

        return output;
    }
}