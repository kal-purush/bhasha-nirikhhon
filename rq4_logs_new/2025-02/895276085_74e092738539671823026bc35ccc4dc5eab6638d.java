package com.spotifyapi.model;

import lombok.Getter;
import lombok.Setter;
import se.michaelthelin.spotify.model_objects.specification.AlbumSimplified;

import java.util.ArrayList;
import java.util.List;

@Getter
@Setter
public class ProgressArtistsUpdate {

    private int processed;
    private int total;
    private List<AlbumSimplified> albums;


    public ProgressArtistsUpdate(int processed, int total) {
        this.processed = processed;
        this.total = total;
        this.albums = null;
    }


    public ProgressArtistsUpdate(int processed, int total, List<AlbumSimplified> albums) {
        this.processed = processed;
        this.total = total;
        this.albums = new ArrayList<>();
    }

}