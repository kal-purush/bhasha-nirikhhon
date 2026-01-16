package com.example.hackathon.tmdb.application;

import com.example.hackathon.dataset.domain.DataField;
import com.example.hackathon.dataset.repository.DataFieldRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.util.UriComponentsBuilder;

import java.util.List;
import java.util.Map;

@Service
public class TmdbService {

    @Value("${tmdb.api.key}")
    private String apiKey;

    private final RestTemplate restTemplate = new RestTemplate();

    @Autowired
    private DataFieldRepository dataFieldRepository;

    public void saveImage(){
        List<DataField> dataFields = dataFieldRepository.findAll();
        for(DataField dataField : dataFields){
            if(dataField.getMediaType().equals("movie")){
                dataField.setImage(getMovieImage(dataField.getTitleName()));
                dataFieldRepository.save(dataField);
            }
            else{
                dataField.setImage(getTvImage(dataField.getTitleName()));
                dataFieldRepository.save(dataField);
            }
        }
    }

    public String getTvImage(String query) {
        String searchUrl = UriComponentsBuilder.fromHttpUrl("https://api.themoviedb.org/3/search/tv")
                .queryParam("api_key", apiKey)
                .queryParam("query", query)
                .queryParam("language", "ko-KR")
                .toUriString();

        Map<String, Object> searchResponse = restTemplate.getForObject(searchUrl, Map.class);

        if (searchResponse != null && !((List<?>) searchResponse.get("results")).isEmpty()) {
            Map<String, Object> firstResult = ((List<Map<String, Object>>) searchResponse.get("results")).get(0);
            String id = String.valueOf(firstResult.get("id"));

            String detailsUrl = UriComponentsBuilder.fromHttpUrl("https://api.themoviedb.org/3/tv/" + id)
                    .queryParam("api_key", apiKey)
                    .queryParam("language", "ko-KR")
                    .toUriString();

            Map<String, Object> detailsResponse = restTemplate.getForObject(detailsUrl, Map.class);

            if (detailsResponse != null && detailsResponse.get("poster_path") != null) {
                String posterPath = String.valueOf(detailsResponse.get("poster_path"));
                return "https://image.tmdb.org/t/p/w500" + posterPath;
            }
        }
        return null;
    }

    // 영화 이미지 가져오기
    public String getMovieImage(String query) {
        String searchUrl = UriComponentsBuilder.fromHttpUrl("https://api.themoviedb.org/3/search/movie")
                .queryParam("api_key", apiKey)
                .queryParam("query", query)
                .queryParam("language", "ko-KR")
                .toUriString();

        Map<String, Object> searchResponse = restTemplate.getForObject(searchUrl, Map.class);

        if (searchResponse != null && !((List<?>) searchResponse.get("results")).isEmpty()) {
            Map<String, Object> firstResult = ((List<Map<String, Object>>) searchResponse.get("results")).get(0);
            String id = String.valueOf(firstResult.get("id"));

            String detailsUrl = UriComponentsBuilder.fromHttpUrl("https://api.themoviedb.org/3/movie/" + id)
                    .queryParam("api_key", apiKey)
                    .queryParam("language", "ko-KR")
                    .toUriString();

            Map<String, Object> detailsResponse = restTemplate.getForObject(detailsUrl, Map.class);

            if (detailsResponse != null && detailsResponse.get("poster_path") != null) {
                String posterPath = String.valueOf(detailsResponse.get("poster_path"));
                return "https://image.tmdb.org/t/p/w500" + posterPath;
            }
        }
        return null;
    }
}