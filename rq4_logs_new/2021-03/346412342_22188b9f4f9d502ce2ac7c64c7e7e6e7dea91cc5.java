package com.hackner.musicompass.controller;

import com.hackner.musicompass.db.ArtistMongoDb;
import com.hackner.musicompass.model.Artist;
import com.hackner.musicompass.model.ArtistInfo;
import com.hackner.musicompass.model.ArtistRelease;
import com.hackner.musicompass.secret.DiscogsSecret;
import com.hackner.musicompass.service.TimeUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.boot.web.server.LocalServerPort;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.RestTemplate;


import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class ArtistControllerTest {

    @LocalServerPort
    private int port;

    private String getUrl() {return "http://localhost:" + port + "api/artist/";}

    @MockBean
    private RestTemplate restTemplate;

    @MockBean
    private DiscogsSecret discogsSecret;

    @Autowired
    private TimeUtils timeUtils;

    @Autowired
    private TestRestTemplate testRestTemplate;

    @Autowired
    private ArtistMongoDb artistMongoDb;

    @BeforeEach
    public void setup(){
        artistMongoDb.deleteAll();
    }

    @Test
    @DisplayName("Get Artist who already exists in DB")
    public void getArtistWhoIsSavedInDb (){
        //GIVEN
        String artistName = "Prince";
        Artist artist = getArtist(artistName);
        artistMongoDb.save(artist);
        when(discogsSecret.getToken()).thenReturn("test");

        //WHEN
        ResponseEntity<Artist> response = testRestTemplate.exchange(getUrl() + artistName, HttpMethod.GET, null, Artist.class);

        //THEN
        assertThat(response.getBody(), equalToObject(artist));
    }

    public Artist getArtist(String artistName){
        Date saveDate = Date.from(timeUtils.now());
        ArtistInfo artistInfo = ArtistInfo.builder()
                .discogsArtistId("111")
                .artistImageUrl("imageUrl")
                .discogsArtistUrl("artistUrl").build();
        ArtistRelease artistRelease1 = ArtistRelease.builder()
                .fullTitle("someTitle")
                .discogsMasterReleaseId("111")
                .format("Album")
                .releaseYear(1999)
                .discogsWant(32768)
                .discogsHave(65536)
                .globalRating(2.0)
                .build();
        ArtistRelease artistRelease2 = ArtistRelease.builder()
                .fullTitle("someOtherTitle")
                .discogsMasterReleaseId("222")
                .format("Single/EP")
                .releaseYear(1999)
                .discogsWant(32768)
                .discogsHave(65536)
                .globalRating(2.0)
                .build();
        List<ArtistRelease> artistAlbums = Arrays.asList(artistRelease1);
        List<ArtistRelease> artistSingles = Arrays.asList(artistRelease2);
        Artist artist = Artist.builder()
                .artistName(artistName)
                .saveDate(saveDate)
                .artistInfo(artistInfo)
                .artistAlbums(artistAlbums)
                .artistSingles(artistSingles).build();
        return artist;
    }


}