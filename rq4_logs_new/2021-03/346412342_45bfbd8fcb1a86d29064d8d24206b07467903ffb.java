package com.hackner.musicompass.service;

import com.hackner.musicompass.discogsapi.model.DiscogsMasterRelease;
import com.hackner.musicompass.model.ArtistRelease;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

@Service
public class ArtistReleaseService {

    public List<ArtistRelease> getSortedReleaseList(String format, List<DiscogsMasterRelease> discogsMasterReleaseList){

        List<String> discogsFormat = convertToDiscogsFormat(format);

        List<ArtistRelease> formatList = discogsMasterReleaseList.stream()
                .filter(discogsMasterRelease -> discogsMasterRelease.getFormat().stream()
                        .anyMatch(discogsFormat::contains)) //this will get a list element from stream and call discogsFormat contains(list element)
                .map(discogsMasterRelease -> new ArtistRelease().builder()
                        .fullTitle(discogsMasterRelease.getFullAlbumTitle())
                        .discogsMasterReleaseId(discogsMasterRelease.getMasterId())
                        .format(format)
                        .releaseYear(discogsMasterRelease.getYear())
                        .discogsWant(discogsMasterRelease.getReleaseStats().getNumberOfWants())
                        .discogsHave(discogsMasterRelease.getReleaseStats().getNumberOfHaves())
                        .globalRating(calculateGlobalRating(discogsMasterRelease.getReleaseStats().getNumberOfHaves(), discogsMasterRelease.getReleaseStats().getNumberOfWants())).build())
                .collect(Collectors.toList());

        List<ArtistRelease> arrayList = new ArrayList<ArtistRelease>(formatList);
        Collections.sort(arrayList);
        List<ArtistRelease> sortedList = arrayList;

        return sortedList;
    }

    public List<String> convertToDiscogsFormat (String format){
        if(format.equals("Album")){
            return Arrays.asList("Album", "LP", "Mini-Album");
        }
        if(format.equals("Single/EP")){
            return Arrays.asList("7\"","10\"","12\"", "EP", "Single", "Maxi-Single");
        }
        return null;
    }

    public double calculateGlobalRating ( int have, int want){
        return want/have*Math.pow(have, 1/8);
    }
}