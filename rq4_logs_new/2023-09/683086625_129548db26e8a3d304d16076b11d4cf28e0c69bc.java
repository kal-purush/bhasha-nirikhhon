package com.singo.Itinerary;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.support.ServletUriComponentsBuilder;

import com.singo.Itinerary.ItineraryModel;

import java.sql.Date;
import java.util.List;
import java.util.stream.Collectors;


@RestController
@RequestMapping("/itineraryController")
@CrossOrigin
public class ItineraryController {

    @Autowired
    private ItineraryService itineraryService;

    //add Itinerary
    @PostMapping("/add")
    public void addItineraryFile( @RequestParam("itineraryName")String itineraryName,@RequestParam("itineraryStart") Date itineraryStart, 
    @RequestParam("itineraryEnd") Date itineraryEnd, @RequestParam("itineraryAdult") String itineraryAdult,
    @RequestParam("itineraryChild") String itineraryChild,@RequestParam("itineraryEmail") String itineraryEmail){
           ItineraryModel itinerary = new ItineraryModel(itineraryName,itineraryStart,itineraryEnd,
           Integer.parseInt(itineraryAdult),Integer.parseInt(itineraryChild),itineraryEmail);
           itineraryService.addItinerary(itinerary);
    }

    @PostMapping("/getAll")
    public ResponseEntity<List<ItineraryModel>> getAllItinerary(@RequestParam("itineraryEmail") String itineraryEmail){
    return ResponseEntity.status(HttpStatus.OK).body(itineraryService.getAllItinerary(itineraryEmail));
    }
}
