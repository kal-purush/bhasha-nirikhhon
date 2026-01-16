package com.upc.model.controller;

import com.upc.model.dto.TemplateDto;
import com.upc.model.service.TemplateService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

/**
 * Created by Qloop on 2017/5/21.
 */
@RestController
public class DisplayController {

    @Autowired
    private TemplateService templateService;

    @RequestMapping(value = "/display", method = RequestMethod.GET)
    public TemplateDto displayTemplate(long id){
        return templateService.getTemplate(id);
    }

}