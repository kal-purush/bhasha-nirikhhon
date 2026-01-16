package com.kraft.atend.service.implementations;

import java.io.IOException;

import org.springframework.stereotype.Service;

import com.kraft.atend.entity.CheckIn;
import com.kraft.atend.model.CheckInDto;
import com.kraft.atend.model.PlaceRegisterDto;
import com.kraft.atend.repository.CheckInRepository;
import com.kraft.atend.repository.PlaceRepository;
import com.kraft.atend.service.abstractions.CheckInHandler;
import com.kraft.atend.service.abstractions.PlaceHandler;

import lombok.RequiredArgsConstructor;

@Service
@RequiredArgsConstructor
public class CheckInService implements CheckInHandler {
	
	private final PlaceHandler placeHandler;
	private final PlaceRepository placeRepository;
	private final CheckInRepository checkInRepository;

	@Override
	public boolean checkIn(CheckInDto checkInDto) throws IOException {
		
		var place = placeRepository.findByLatitudeAndLongitude(checkInDto.latitude(), checkInDto.longitude()).orElse(null);
		
		if(!place.equals(null)) {
			var entity = new CheckIn();
			entity.setPlaceId(place.getId());
			entity.setUserId(checkInDto.userId());
			return !checkInRepository.save(entity).equals(null);
		}
		
		var dto = new PlaceRegisterDto(
				checkInDto.placeName(),
				checkInDto.latitude(),
				checkInDto.longitude(),
				checkInDto.rawFile());
		var id =  placeHandler.registerNewPlace(dto);
		
		var entity = new CheckIn();
		entity.setPlaceId(id);
		entity.setUserId(checkInDto.userId());
		
		return !checkInRepository.save(entity).equals(null);
	}

}