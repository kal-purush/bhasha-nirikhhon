package org.citybike;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Page;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class StationController {

    @Autowired
    private StationService stationService;

    @GetMapping("/stations")
    public Page<Station> getStations(@RequestParam(required = false, defaultValue = "0") Integer page,
                                     @RequestParam(required = false) String name) {
        if (name != null && name.length() > 0) {
            return stationService.getAllStationsWithName(page,name);
        }
        return stationService.getAllStations(page);
    }

    @GetMapping("/stations/{id}")
    public Station getStation(@PathVariable Long id) {
        return stationService.getOneStationById(id);
    }
}