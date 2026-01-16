package ar.edu.unq.tsp.controller;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;

@CrossOrigin(origins = {"*"})
@Controller
public class HelpController {

    @GetMapping("/")
    public ResponseEntity<String> hi() {
        return new ResponseEntity<String>("Hi!", HttpStatus.OK);
    }

}