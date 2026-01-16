package org.citybike.controller;

import org.citybike.dto.StationRequest;
import org.citybike.entity.Station;
import org.citybike.service.StationService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Page;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import javax.validation.Valid;

@CrossOrigin
@RestController
@RequestMapping("/api/stations")
public class StationController {

    @Autowired
    private StationService stationService;


    @GetMapping("")
    public ResponseEntity<Page<Station>> getStations(@RequestParam(required = false, defaultValue = "0") Integer page,
                                     @RequestParam(required = false) String filter,
                                     @RequestParam(required = false, defaultValue = "10") Integer stationsPerPage) {
        if (filter != null && filter.length() > 0) {
            return ResponseEntity.ok(stationService.getAllStationsWithName(page,stationsPerPage,filter));
        }
        return ResponseEntity.ok(stationService.getAllStations(page,stationsPerPage));
    }

    @GetMapping("/{id}")
    public ResponseEntity<Station> getStation(@PathVariable Long id) {
        return ResponseEntity.ok(stationService.getOneStationById(id));
    }

    @PostMapping("")
    public ResponseEntity<Station> postStation(@RequestBody StationRequest stationRequest) {
        return new ResponseEntity<>(
                stationService.saveStation(stationRequest),
                HttpStatus.CREATED);
    }

    @DeleteMapping("/{id}")
    public ResponseEntity<Station> deleteStation(@PathVariable Long id) {
        return ResponseEntity.ok(stationService.removeStation(id));
    }
}