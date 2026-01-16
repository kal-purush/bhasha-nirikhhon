package com.endava.jbehave;

import com.wrapper.spotify.SpotifyApi;
import com.wrapper.spotify.exceptions.SpotifyWebApiException;
import com.wrapper.spotify.model_objects.specification.Album;
import com.wrapper.spotify.requests.data.albums.GetAlbumRequest;
import net.thucydides.core.annotations.Step;

import java.io.IOException;

import static org.junit.Assert.assertEquals;

public class AlbumSteps {

    private SpotifyApi spotifyApi = Configuration.spotifyInstance();

    private GetAlbumRequest getAlbumRequest;
    private Album album;

    @Step("Given an id that belongs to Un dia normal album from Juanes")
    public void an_id_that_belongs_to_un_dia_normal_album_from_juanes(String albumId) {
        getAlbumRequest = spotifyApi.getAlbum(albumId).build();
    }

    @Step("Given a fake album id")
    public void a_fake_album_id(String fakeAlbumId) {
        getAlbumRequest = spotifyApi.getAlbum(fakeAlbumId).build();
    }

    @Step("When user executes album api")
    public void user_executes_album_api() {
        try {
            album = getAlbumRequest.execute();
        }
        catch (IOException e) { }
        catch (SpotifyWebApiException e) { }
    }

    @Step("When user executes album api it will throw exception")
    public void user_executes_album_api_it_will_throw_exception() throws Throwable {
        album = getAlbumRequest.execute();
    }

    @Step("Then user gets Un Día Normal (Europe Version) name")
    public void user_gets_un_dia_normal_europe_version_name() {
        assertEquals(album.getName(), "Un Día Normal (Europe Version)");
    }

    @Step("Then user gets Juanes name from album")
    public void user_gets_juanes_name_from_album() {
        assertEquals(album.getArtists()[0].getName(), "Juanes");
    }

    @Step("Then user gets album NullPointerException")
    public void user_gets_album_nullpointerexception() throws Throwable {
        album.getArtists()[0].getName();
    }

}