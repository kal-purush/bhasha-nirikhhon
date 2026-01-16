package com.zakrzewski.intentionbook.controllers;

import com.zakrzewski.intentionbook.entities.BookOfIntentionModel;
import com.zakrzewski.intentionbook.services.BookOfIntentionsServiceImpl;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.InputStreamSource;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;

import java.io.ByteArrayInputStream;
import java.time.LocalDate;
import java.util.List;


@Controller
public class IntentionDownloadController {

    private final BookOfIntentionsServiceImpl bookOfIntentionsService;

    @Autowired
    public IntentionDownloadController(BookOfIntentionsServiceImpl bookOfIntentionsService) {
        this.bookOfIntentionsService = bookOfIntentionsService;
    }


    @RequestMapping("/intention-generate")
    public String generateIntentionPage(Model model){
        LocalDate localDate = LocalDate.now();
        model.addAttribute("minDate", localDate);
        return "intention-generate";
    }


    @RequestMapping(value = "/generate", method = RequestMethod.GET, produces = MediaType.APPLICATION_PDF_VALUE)
    public ResponseEntity<InputStreamSource> generateIntentionFile(){
        List<BookOfIntentionModel> bookListIntention = bookOfIntentionsService.getAllIntentions();

        return null;
    }
}