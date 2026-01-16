package com.example.demo.base;

import com.example.demo.constant.MessageConstant;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

/**
 * @author Sombath
 * create at 23/10/22 6:01 PM
 */
public class BaseController {

    protected ResponseEntity<StructureRS> response(StructureRS structureRS) {
        return ResponseEntity
                .status(structureRS.getStatus())
                .body(structureRS);
    }

    protected ResponseEntity<StructureRS> response() {
        return ResponseEntity
                .status(HttpStatus.OK)
                .body(new StructureRS(HttpStatus.OK, MessageConstant.SUCCESSFULLY));
    }

    protected ResponseEntity<StructureRS> response(Object date) {
        return ResponseEntity
                .status(HttpStatus.OK)
                .body(new StructureRS(HttpStatus.OK, MessageConstant.SUCCESSFULLY, date));
    }

    protected ResponseEntity<StructureRS> response(Object date, PagingRS pagingRS) {
        return ResponseEntity
                .status(HttpStatus.OK)
                .body(new StructureRS(date, pagingRS));
    }

}