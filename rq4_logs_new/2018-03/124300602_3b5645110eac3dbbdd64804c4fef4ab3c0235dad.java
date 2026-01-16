package com.endava.jbehave;

import com.google.gson.JsonParser;
import com.wrapper.spotify.SpotifyApi;
import com.wrapper.spotify.exceptions.SpotifyWebApiException;
import com.wrapper.spotify.model_objects.miscellaneous.CurrentlyPlayingContext;
import com.wrapper.spotify.requests.data.player.GetInformationAboutUsersCurrentPlaybackRequest;
import com.wrapper.spotify.requests.data.player.StartResumeUsersPlaybackRequest;
import net.thucydides.core.annotations.Step;
import org.junit.Assert;

import java.io.IOException;

public class PlayerSteps {

    private SpotifyApi spotifyApi = Configuration.spotifyAuthorizationInstance();

    private StartResumeUsersPlaybackRequest startResumeUsersPlaybackRequest;
    private String msg;

    private GetInformationAboutUsersCurrentPlaybackRequest getInformationAboutUsersCurrentPlaybackRequest;
    private CurrentlyPlayingContext currentlyPlayingContext;

    @Step
    public void an_uri_that_belongs_to_a_dios_le_pido_track(String uri) {
        startResumeUsersPlaybackRequest = spotifyApi.startResumeUsersPlayback()
                .uris(new JsonParser().parse("[\""+uri+"\"]").getAsJsonArray())
                .build();
    }

    @Step
    public void user_executes_player_api() {
        try {
            msg = startResumeUsersPlaybackRequest.execute();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (SpotifyWebApiException e) {
            e.printStackTrace();
        }
    }

    @Step
    public void user_listens_a_dios_le_pido_track() {
        getInformationAboutUsersCurrentPlaybackRequest = spotifyApi.getInformationAboutUsersCurrentPlayback().build();
        try {
            currentlyPlayingContext = getInformationAboutUsersCurrentPlaybackRequest.execute();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (SpotifyWebApiException e) {
            e.printStackTrace();
        }
        Assert.assertTrue(currentlyPlayingContext.getIs_playing());
    }

}