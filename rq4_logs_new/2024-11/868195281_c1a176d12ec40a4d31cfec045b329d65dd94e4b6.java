package com.filegenie.backend.Controllers;

import com.filegenie.backend.DTO.HttpException;
import com.filegenie.backend.Entities.Field;
import com.filegenie.backend.Entities.User;
import com.filegenie.backend.Services.JsonParserService;
import com.filegenie.backend.Services.RegisterFieldService;
import com.filegenie.backend.Services.UserService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;

import java.io.File;
import java.util.List;

@Controller
@RestController
@RequestMapping("/api/register")
public class RegisterFieldController {

    @Autowired
    private JsonParserService jsonParserService;

    @Autowired
    private RegisterFieldService registerFieldService;

    @Autowired
    private UserService userService;

    @PostMapping(path = "/newFields")
    public ResponseEntity<HttpStatus> AddNewField(
            @RequestParam("file") MultipartFile jsonFile,
            @RequestHeader("Authorization") String token) {

        User user;
        try {
            user = userService.getAuthedUser(token);
        } catch (Exception e) {
            String customMessage = "Token introuvable  : " + e.getMessage();
            System.err.println(customMessage);
            e.printStackTrace();
            return new ResponseEntity<>(HttpStatus.UNAUTHORIZED);
        }

        try {

            File tempFile = File.createTempFile("uploaded-", ".json");
            jsonFile.transferTo(tempFile);
            List<Field> list = jsonParserService.parseJsonToFields(tempFile);
            System.out.println(list);
            registerFieldService.saveFields(list);
            tempFile.delete();

        } catch (Exception e) {
            String customMessage = "Erreur lors de l'enregistrement des champs : " + e.getMessage();
            System.err.println(customMessage);
            e.printStackTrace();
            return new ResponseEntity<>(HttpStatus.INTERNAL_SERVER_ERROR);
        }

        return new ResponseEntity<>(HttpStatus.CREATED);
    }
}