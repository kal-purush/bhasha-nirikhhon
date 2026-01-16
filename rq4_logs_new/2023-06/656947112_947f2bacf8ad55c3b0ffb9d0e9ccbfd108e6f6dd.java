package com.ar.conde.reservaDeTurnos.controller;
import com.ar.conde.reservaDeTurnos.entity.Domicilio;
import com.ar.conde.reservaDeTurnos.service.IService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Optional;

@RestController
public class DomicilioController {


        @Autowired
        @Qualifier("domicilio")
        private IService service;

        @GetMapping("/api/domicilios")
        public List<Domicilio> getAll(){
            return service.getAll();
        }

        @GetMapping("/api/domicilios/{id}")
        public ResponseEntity<?> domicilio(@PathVariable Long id){
            Optional<Domicilio> domicilioOptional = service.getById(id);
            if(domicilioOptional.isPresent()){
                return ResponseEntity.ok(domicilioOptional.get());
            }else {
                return ResponseEntity.notFound().build();
            }
        }

        @PostMapping("/api/domicilios")
        public ResponseEntity<?> save(@RequestBody Domicilio domicilio){
            return ResponseEntity.status(HttpStatus.CREATED).body(service.create(domicilio));
        }

        @PutMapping("/api/domicilios/{id}")
        public ResponseEntity<?> update(@RequestBody Domicilio domicilio, @PathVariable Long id){

            return ResponseEntity.status(HttpStatus.CREATED).body(service.update(id, domicilio));

        }

        @DeleteMapping("/api/domicilios/{id}")
        public ResponseEntity<?> delete(@PathVariable Long id){
            Optional<Domicilio> domicilio = service.getById(id);
            if(domicilio.isPresent()){
                service.delete(id);
                return  ResponseEntity.noContent().build();
            }else {
                return ResponseEntity.notFound().build();
            }
        }
}