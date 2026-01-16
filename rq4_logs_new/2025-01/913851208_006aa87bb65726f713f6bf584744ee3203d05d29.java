package com.project_merge.jigu_travel.api.Place.service;

import com.project_merge.jigu_travel.api.Place.model.Place;
import com.project_merge.jigu_travel.api.Place.repository.PlaceRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Optional;

@Service
@RequiredArgsConstructor
public class LocationService {

    private final PlaceRepository placeRepository;

    public List<Place> findNearbyPlaces(double latitude, double longitude, double radius) {
        return placeRepository.findNearbyPlaces(latitude, longitude, radius);
    }

    public Optional<Place> findPlaceById(Long placeId) {
        return placeRepository.findById(placeId);
    }

}