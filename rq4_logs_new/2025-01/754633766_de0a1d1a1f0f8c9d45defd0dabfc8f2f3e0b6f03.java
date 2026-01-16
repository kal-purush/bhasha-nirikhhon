package com.kraft.atend.service.implementations;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.client.RestTemplate;

import com.kraft.atend.entity.OsmObject;
import com.kraft.atend.repository.OsmRepository;
import com.kraft.atend.service.abstractions.OsmHandler;

import lombok.RequiredArgsConstructor;

@Service
@RequiredArgsConstructor
public class OsmService implements OsmHandler {

	@Value("${osm.reverse.get}")
	private String osmReverseApi;

	private final RestTemplate restTemplate;
	private final OsmRepository osmRepository;

	@Override
	public long confirmLocation(OsmObject osmObject) {
		var foundRecord = osmRepository.findByPlaceId(osmObject.getPlaceId()).orElse(null);
		if(foundRecord.equals(null)) {
			return osmRepository.save(osmObject).getPlaceId();
		}
		return foundRecord.getPlaceId();
	}

	@Override
	public OsmObject callOsmApi(String latitude, String longitude) {
		return restTemplate.getForObject(String.format(osmReverseApi, latitude, longitude), OsmObject.class);
	}

}