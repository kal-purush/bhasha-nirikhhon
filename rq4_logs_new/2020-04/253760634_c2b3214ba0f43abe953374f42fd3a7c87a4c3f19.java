package com.example.airbnbapi.repository;


import com.example.airbnbapi.model.Media;
import com.example.airbnbapi.model.MediaType;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Component
public class RepositoryFactory<M extends Media> {

    Map<MediaType, MediaRepository<M>> map;

    @Autowired
    public RepositoryFactory(List<MediaRepository<M>> mediaRepositories){
        this.map = new HashMap<>();

        mediaRepositories.stream().filter(mediaRepository -> mediaRepository.getMediaType() != null).forEach(mediaRepository -> map.put(mediaRepository.getMediaType(),mediaRepository));

    }


    public MediaRepository<M> getRepositoryByType(MediaType type) {

     return map.get(type);
    }
}

