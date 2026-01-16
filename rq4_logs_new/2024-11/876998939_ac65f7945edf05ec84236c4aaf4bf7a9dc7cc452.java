package com.smartverse.churchlitebackend.handlers.userconfiguration;

import com.smartverse.churchlitebackend.repository.userconfiguration.UserConfigurationCustomRepository;
import com.smartverse.churchlitebackend_gen.GetUser;
import com.smartverse.churchlitebackend_gen.GetUserOutput;
import com.smartverse.churchlitebackend_gen.UserConfigurationDTOConverter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.RestController;

import java.util.UUID;

@RestController
@CrossOrigin(origins="*")
public class UserConfigurationCustomHandlerImpl implements GetUser {

    @Autowired
    UserConfigurationCustomRepository userConfigurationCustomRepository;

    @Autowired
    UserConfigurationDTOConverter userConfigurationDTOConverter;

    @Override
    public ResponseEntity<GetUserOutput> getUser(UUID hash) {
        var user = userConfigurationCustomRepository.findByHash(hash);
        var output = new GetUserOutput();
        user.ifPresent(item -> {
            output.output = userConfigurationDTOConverter.toDTO(item, null);
        });
        return ResponseEntity.ok(output);
    }
}