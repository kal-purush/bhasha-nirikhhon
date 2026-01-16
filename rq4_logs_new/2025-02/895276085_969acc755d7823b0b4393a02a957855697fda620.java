package com.spotifyapi.service;

import com.spotifyapi.model.SpotifyArtist;
import se.michaelthelin.spotify.model_objects.specification.AlbumSimplified;
import se.michaelthelin.spotify.model_objects.specification.PlaylistTrack;
import se.michaelthelin.spotify.model_objects.specification.TrackSimplified;

import java.util.List;

public interface PaginationService {

    List<SpotifyArtist> paginationOfArtists();

    List<AlbumSimplified> paginationOfReleasesArtist(String artistId, Long releaseOfDay);

    List<TrackSimplified> paginationOfSaveReleasesMethod(String albumId);

    List<PlaylistTrack> paginationOfDeleteReleasesMethod(String playlistId);

}