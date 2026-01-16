package com.endava.jbehave;

import com.wrapper.spotify.SpotifyApi;
import com.wrapper.spotify.exceptions.SpotifyWebApiException;
import com.wrapper.spotify.model_objects.credentials.ClientCredentials;
import com.wrapper.spotify.model_objects.specification.Artist;
import com.wrapper.spotify.requests.authorization.client_credentials.ClientCredentialsRequest;
import com.wrapper.spotify.requests.data.artists.GetArtistRequest;

import java.io.IOException;

public class Configuration {

    public static void main(String[] args) {
        SpotifyApi spotifyApi = new SpotifyApi.Builder()
                .setClientId("6d27a1bcfddc4d8b909a415f78ea1233")
                .setClientSecret("e3d01bbb6b634760a3a7bc41e337966d")
                .build();

        ClientCredentialsRequest clientCredentialsRequest = spotifyApi.clientCredentials()
                .build();

        ClientCredentials clientCredentials;

        {
            try {
                clientCredentials = clientCredentialsRequest.execute();
                spotifyApi.setAccessToken(clientCredentials.getAccessToken());
                System.out.println("Expires in: " + clientCredentials.getExpiresIn());
            } catch (IOException e) {
                e.printStackTrace();
            } catch (SpotifyWebApiException e) {
                e.printStackTrace();
            }
        }

        //Recuperando un artista

        //Juanes
        GetArtistRequest getArtistRequest = spotifyApi.getArtist("0UWZUmn7sybxMCqrw9tGa7").build();
        Artist artist;

        {
            try {
                artist = getArtistRequest.execute();

                System.out.println(artist.getName());
                System.out.println(artist.getGenres());
            } catch (IOException e) {
                e.printStackTrace();
            } catch (SpotifyWebApiException e) {
                e.printStackTrace();
            }
        }

        // https://github.com/thelinmichael/spotify-web-api-java
    }

}