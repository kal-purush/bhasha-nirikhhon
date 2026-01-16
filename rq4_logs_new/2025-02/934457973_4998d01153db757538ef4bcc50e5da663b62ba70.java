package com.proyectofinal.clave_compas.controller;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.proyectofinal.clave_compas.dto.InstrumentoDto;
import com.proyectofinal.clave_compas.exception.NotValidInstrumentData;
import com.proyectofinal.clave_compas.exception.ProductAlreadyOnRepositoryException;
import com.proyectofinal.clave_compas.model.Instrumento;
import com.proyectofinal.clave_compas.service.ImageServices;
import com.proyectofinal.clave_compas.service.ProductServices;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;

import java.io.FileNotFoundException;

@RestController
@RequestMapping("/api/v1/productos")
public class ProductController {

    private final ProductServices productServices;

    private final ImageServices imageServices;

    public ProductController(@Autowired ProductServices productServices, ImageServices imageServices) {
        this.productServices = productServices;
        this.imageServices = imageServices;
    }

    @PostMapping
    private ResponseEntity<Instrumento> registrarProducto(@RequestParam String instrumentoDtoJson,
                                                          @RequestParam("file") MultipartFile file) throws NotValidInstrumentData, FileNotFoundException {
        try {
            ObjectMapper objectMapper = new ObjectMapper();
            InstrumentoDto instrumentoDto = objectMapper.readValue(instrumentoDtoJson, InstrumentoDto.class);
            if (!instrumentoDto.isValid()) throw new NotValidInstrumentData("Argumentos no validos");
            if (file.isEmpty()) throw new FileNotFoundException();

            Instrumento instrumento = instrumentoDto.toEntity();
            imageServices.saveImage(file, instrumento);
            productServices.saveProduct(instrumento, instrumentoDto);
            return ResponseEntity.ok(instrumento);
        } catch (ProductAlreadyOnRepositoryException e) {
            return ResponseEntity.badRequest().build();
        } catch (JsonMappingException e) {
            throw new RuntimeException(e);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }

    }
}