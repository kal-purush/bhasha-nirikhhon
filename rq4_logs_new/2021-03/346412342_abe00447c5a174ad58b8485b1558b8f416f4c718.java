package com.hackner.musicompass.helper;

import com.hackner.musicompass.discogsapi.model.*;
import com.hackner.musicompass.model.Artist;
import com.hackner.musicompass.model.ArtistInfo;
import com.hackner.musicompass.model.ArtistRelease;
import com.hackner.musicompass.service.TimeUtils;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.Arrays;
import java.util.Date;
import java.util.List;

public class TestData {

    public Artist getArtist(String artistName, Date saveDate){
        //Date saveDate = Date.from(timeUtils.now());
        ArtistInfo artistInfo = ArtistInfo.builder()
                .discogsArtistId("111")
                .artistImageUrl("imageUrl")
                .discogsArtistUrl("artistUrl").build();
        ArtistRelease artistRelease1 = ArtistRelease.builder()
                .fullTitle("someTitle")
                .discogsMasterReleaseId("111")
                .format("Album")
                .coverImageUrl("coverImageUrl")
                .releaseYear(1999)
                .discogsWant(32768)
                .discogsHave(65536)
                .globalRating(2.0)
                .build();
        ArtistRelease artistRelease2 = ArtistRelease.builder()
                .fullTitle("someOtherTitle")
                .discogsMasterReleaseId("222")
                .format("Single/EP")
                .coverImageUrl("someOtherCoverImageUrl")
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

    public DiscogsMasterReleaseSearchResults getDiscogsMasterReleaseSearchResults(){

        DiscogsMasterRelease discogsMasterRelease1 = DiscogsMasterRelease.builder()
                .fullAlbumTitle("someTitle")
                .masterId("111")
                .format(Arrays.asList("Album","CD"))
                .coverImageUrl("coverImageUrl")
                .year(1999)
                .releaseStats(Stats.builder().numberOfWants(32768).numberOfHaves(65536).build())
                .build();
        DiscogsMasterRelease discogsMasterRelease2 = DiscogsMasterRelease.builder()
                .fullAlbumTitle("someOtherTitle")
                .masterId("222")
                .format(Arrays.asList("Single","7\""))
                .coverImageUrl("someOtherCoverImageUrl")
                .year(1999)
                .releaseStats(Stats.builder().numberOfWants(32768).numberOfHaves(65536).build())
                .build();

        DiscogsPagination discogsPagination = new DiscogsPagination(2,1,1);

        DiscogsMasterReleaseSearchResults discogsMasterReleaseSearchResults = DiscogsMasterReleaseSearchResults.builder()
                .pagination(discogsPagination)
                .results(Arrays.asList(discogsMasterRelease1,discogsMasterRelease2)).build();

        return discogsMasterReleaseSearchResults;
    }

    public DiscogsArtistSearchResults getDiscogsArtistSearchResults (String artistName){

        String discogsArtistId = "111";
        String artistImageUrl = "imageUrl";
        String discogsArtistUrl = "artistUrl";

        DiscogsArtist discogsArtist = DiscogsArtist.builder()
                .discogsArtistId(discogsArtistId)
                .artistName(artistName)
                .artistImageUrl(artistImageUrl)
                .discogsArtistUrl(discogsArtistUrl).build();
        DiscogsArtistSearchResults discogsArtistSearchResults = DiscogsArtistSearchResults.builder()
                .results(Arrays.asList(discogsArtist)).build();

        return discogsArtistSearchResults;
    }

}