package com.apple.playlistbuilder;

import org.junit.Assert;
import org.mockito.Mockito;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;
import org.testng.collections.Lists;

import java.io.BufferedReader;
import java.util.List;

import static org.mockito.Mockito.when;

/**
 * Created by cpereira on 09/07/17.
 */
public class PlaylistBuilderTest {

    PlaylistBuilder playlistBuilder;
    BufferedReader mockReader;


    @BeforeMethod
    private void setUp(){
        mockReader = Mockito.mock(BufferedReader.class);
        playlistBuilder = new PlaylistBuilder(mockReader);
    }

    @Test
    public void emptyFile() throws Exception{

        when(mockReader.readLine()).thenReturn(null);
        List<String> expectedPlaylist = Lists.newArrayList("Total Time: 00:00");

        Assert.assertEquals("Playlist expected to be empty", expectedPlaylist,
                playlistBuilder.generatePlaylist());
    }

    @Test
    public void singleMusicOneAlbum() throws Exception{

        when(mockReader.readLine()).thenReturn(
                "XTC / Drums and Wires / 1979",
                "Making Plans for Nigel - 4:14",
                null);

        List<String> expectedPlaylist = Lists.newArrayList(
                "XTC - Drums and Wires - 1979 - Making Plans for Nigel - 04:14",
                "Total Time: 04:14");

        Assert.assertEquals(expectedPlaylist, playlistBuilder.generatePlaylist());
    }

    @Test
    public void multipleMusicsSingleAlbum() throws Exception{

        when(mockReader.readLine()).thenReturn(
                "XTC / Drums and Wires / 1979",
                "Making Plans for Nigel - 4:14",
                "Helicopter - 3:55",
                "Day In Day Out - 3:08",
                "When You're Near Me I Have Difficulty - 3:22",
                null);

        List<String> expectedPlaylist = Lists.newArrayList(
                "XTC - Drums and Wires - 1979 - Day In Day Out - 03:08",
                "XTC - Drums and Wires - 1979 - When You're Near Me I Have Difficulty - 03:22",
                "Total Time: 06:30");

        Assert.assertEquals(expectedPlaylist, playlistBuilder.generatePlaylist());
    }

    @Test
    public void multipleAlbums() throws Exception{

        when(mockReader.readLine()).thenReturn(
                "XTC / Drums and Wires / 1979",
                "Making Plans for Nigel - 4:14",
                "Helicopter - 3:55",
                "",
                "Vampire Weekend / Contra / 2010",
                "Horchata - 3:27",
                "White Sky - 2:59",
                "",
                "Love / Forever Changes / 1967",
                "Alone Again Or - 3:17",
                "A House Is Not A Motel - 3:32",
                null);

        List<String> expectedPlaylist = Lists.newArrayList(
                "Vampire Weekend - Contra - 2010 - White Sky - 02:59",
                "Love - Forever Changes - 1967 - Alone Again Or - 03:17",
                "Vampire Weekend - Contra - 2010 - Horchata - 03:27",
                "Total Time: 09:43");

        Assert.assertEquals(expectedPlaylist, playlistBuilder.generatePlaylist());
    }

    @Test
    public void multipleAlbumsRepeatedDuration() throws Exception{

        when(mockReader.readLine()).thenReturn(
                "XTC / Drums and Wires / 1979",
                "Making Plans for Nigel - 4:14",
                "Helicopter - 3:55",
                "",
                "Vampire Weekend / Contra / 2010",
                "Horchata - 3:27",
                "White Sky - 2:59",
                "",
                "Love / Forever Changes / 1967",
                "Alone Again Or - 3:17",
                "A House Is Not A Motel - 3:27",
                null);

        List<String> expectedPlaylist = Lists.newArrayList(
                "Vampire Weekend - Contra - 2010 - White Sky - 02:59",
                "Love - Forever Changes - 1967 - Alone Again Or - 03:17",
                "Vampire Weekend - Contra - 2010 - Horchata - 03:27",
                "Love - Forever Changes - 1967 - A House Is Not A Motel - 03:27",
                "Total Time: 13:10");

        Assert.assertEquals(expectedPlaylist, playlistBuilder.generatePlaylist());
    }

}