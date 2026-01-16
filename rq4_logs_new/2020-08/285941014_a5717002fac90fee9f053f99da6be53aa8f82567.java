package lk.ac.cmb.ucsc.scs3203.miniproject.backend.controller.system;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/v1")
public class Health {

    @GetMapping
    public ResponseEntity<String> getHealth(){
        return new ResponseEntity<>("Running", HttpStatus.OK);
    }

}