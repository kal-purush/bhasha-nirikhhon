package com.rogih.userauth.controller;

import com.rogih.userauth.dtos.InstructorDto;
import com.rogih.userauth.enums.UserType;
import com.rogih.userauth.models.UserModel;
import com.rogih.userauth.services.UserService;
import lombok.extern.log4j.Log4j2;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import javax.validation.Valid;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Optional;

@Log4j2
@RestController
@CrossOrigin(origins = "*", maxAge = 3600)
@RequestMapping("/instructors")
public class InstructorController {

    @Autowired
    UserService userService;

    @PostMapping("/subscription")
    public ResponseEntity<Object> saveSubscriptionInstructor(@RequestBody @Valid InstructorDto instructorDto){
        log.info("Iniciando o processo de salvar um usuário como instrutor.");
        Optional<UserModel> userModelOptional = userService.findById(instructorDto.getUserId());
        if(!userModelOptional.isPresent()) {
            log.error("User {} not found.", instructorDto.getUserId());
            return ResponseEntity.status(HttpStatus.NOT_FOUND).body("User not found.");
        }else {
            var userModel = userModelOptional.get();
            userModel.setUserType(UserType.INSTRUCTOR);
            userModel.setLastUpdateDate(LocalDateTime.now(ZoneId.of("UTC")));
            userService.save(userModel);
            log.info("O usuário {} foi salvo como instrutor.", instructorDto.getUserId());
            return ResponseEntity.status(HttpStatus.OK).body(userModel);
        }
    }

}