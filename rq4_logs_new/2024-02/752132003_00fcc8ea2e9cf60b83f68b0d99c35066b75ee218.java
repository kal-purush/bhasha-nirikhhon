package com.web.controller;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.web.domain.WbDTO;
import com.web.service.WorkbookService;

@RestController
@RequestMapping("/lecture")
public class WorkbookController {

    @Autowired
    private WorkbookService workbookService;

    @GetMapping
    public List<WbDTO> getAllWorkbooks() {
        return workbookService.getAllWorkbooks();
    }
}