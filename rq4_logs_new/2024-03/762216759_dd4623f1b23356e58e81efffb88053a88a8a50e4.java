package com.kafkadiscovery.api.controller;

import java.io.IOException;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import org.springframework.web.multipart.MultipartFile;
// import com.fasterxml.jackson.dataformat.xml.XmlMapper;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;

import lombok.RequiredArgsConstructor;

@RestController
@RequestMapping("/api")
@RequiredArgsConstructor
public class MainController {
     
   @PostMapping("/data")
    public ResponseEntity<String> uploadFile(@RequestParam("file") MultipartFile file) {
        // try {
           // Création d'un XmlMapper pour lire le fichier XML
        //    XmlMapper xmlMapper = new XmlMapper();
        //    // Conversion du fichier XML en arbre JsonNode
        //    JsonNode node = xmlMapper.readTree(file.getInputStream());

        //    // Création d'un ObjectMapper pour écrire l'arbre JsonNode en tant que JSON
        //    ObjectMapper objectMapper = new ObjectMapper();
        //    String json = objectMapper.writeValueAsString(node);
            System.out.println("test");
           // Retourne le JSON résultant
           return ResponseEntity.ok("json");
    //    } catch (IOException e) {
    //        e.printStackTrace();
    //        return ResponseEntity.status(500).body("Erreur lors de la conversion du fichier XML en JSON");
    //    }
    }
}