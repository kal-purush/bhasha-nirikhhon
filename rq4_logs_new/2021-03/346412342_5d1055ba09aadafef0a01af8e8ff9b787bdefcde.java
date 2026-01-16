package com.hackner.musicompass.service;
import com.hackner.musicompass.db.ArtistMongoDb;
import com.hackner.musicompass.discogsapi.model.DiscogsArtist;
import com.hackner.musicompass.discogsapi.model.DiscogsArtistSearchResults;
import com.hackner.musicompass.model.ArtistInfo;
import com.hackner.musicompass.discogsapi.service.DiscogsArtistApiService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import java.util.List;
import java.util.stream.Collectors;

@Service
public class ArtistInfoService {
    private final DiscogsArtistApiService discogsArtistApiService;

    @Autowired
    public ArtistInfoService(DiscogsArtistApiService discogsArtistApiService) {
        this.discogsArtistApiService = discogsArtistApiService;
    }

    public List<ArtistInfo> getArtistInfoListBySearchTerm(String searchTerm) {
        DiscogsArtistSearchResults discogsArtistSearchResults = discogsArtistApiService.getDiscogsArtistByName(searchTerm);

        List<DiscogsArtist> discogsArtistList = discogsArtistSearchResults.getResults();

        if (discogsArtistList.size() > 5) {
            discogsArtistList = discogsArtistList.subList(0, 5);
        }

        List<ArtistInfo> artistInfoList = discogsArtistList.stream()
                .map(discogsArtist -> {
                    ArtistInfo artistInfo = new ArtistInfo().builder()
                            .artistName(discogsArtist.getArtistName())
                            .artistImageUrl(discogsArtist.getArtistImageUrl())
                            .discogsArtistId(discogsArtist.getDiscogsArtistId())
                            .discogsArtistUrl(discogsArtist.getDiscogsArtistUrl()).build();
                    return artistInfo;
                })
                .collect(Collectors.toList());

        return artistInfoList;

    }
}