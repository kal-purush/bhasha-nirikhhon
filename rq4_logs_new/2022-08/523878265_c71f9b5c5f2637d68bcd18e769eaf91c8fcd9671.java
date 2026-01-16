package org.citybike;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Page;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class JourneyController {

    @Autowired
    private JourneyService journeyService;

    @GetMapping("/journeys")
    public Page<Journey> getJourneys(@RequestParam(required = false,defaultValue = "0") int page,
                                     @RequestParam(required = false) String filter) {
        if (filter != null && filter.length() > 0) {
            return journeyService.getAllFilteredJourneys(page,filter);
        }
        return journeyService.getAllJourneys(page);
    }
}