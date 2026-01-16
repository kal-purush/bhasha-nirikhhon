package org.citybike;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Page;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
public class DataController {

    @Autowired

    private DataService dataService;


    @GetMapping("/dataseed")
    public void seedData() {
        dataService.addCSVStationsDataToDb();
    }

    @GetMapping("/testseed/page/{page}")
    public Page<Station> getStation(@PathVariable int page) {
        return dataService.getAllStations(page);
    }
}