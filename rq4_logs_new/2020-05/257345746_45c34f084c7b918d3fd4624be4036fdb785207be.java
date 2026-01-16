package pl.pawel.weatherapp.service;

import org.springframework.stereotype.Service;
import pl.pawel.weatherapp.model.response.CurrentWeatherResponse;
import pl.pawel.weatherapp.repository.CurrentWeatherRepository;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

@Service
public class CurrentWeatherResponseService {

    private CurrentWeatherRepository currentWeatherRepository;

    public CurrentWeatherResponseService(CurrentWeatherRepository currentWeatherRepository) {
        this.currentWeatherRepository = currentWeatherRepository;
    }


    public void addCurrentWeatherResponse(CurrentWeatherResponse weatherResponse){
        currentWeatherRepository.save(weatherResponse);
    }

    public void formatDate(CurrentWeatherResponse currentWeatherResponse){
        final DateTimeFormatter formatter = DateTimeFormatter.ofPattern("dd MMMM yyyy HH:mm:ss");
        String date = Instant.ofEpochSecond(currentWeatherResponse.getDt()).atZone(ZoneId.of("GMT+2")).format(formatter);
        currentWeatherResponse.setConvertedDate(date);
    }
}