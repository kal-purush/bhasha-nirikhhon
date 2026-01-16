package com.apple.playlistbuilder;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;

public class Application {
    @Parameter(names = "-input", description = "Albums input file")
    private String inputFile = "Albums.txt";

    @Parameter(names = "-output", description = "Playlist output file")
    private String outputFile = "Playlist.txt";

    @Parameter(names = "--help", help = true)
    private boolean help = false;

    public static void main(String[] args) throws Exception {
        Application app = new Application();
        JCommander commandArgs = JCommander.newBuilder()
                .addObject(app)
                .programName("Playlist Builder App")
                .build();
        commandArgs.parse(args);

        if (app.help) {
            commandArgs.usage();
            return;
        }
        PlaylistBuilder builder = new PlaylistBuilder(app.inputFile);
        Playlist playlist = builder.generatePlaylist();
        builder.writePlaylistFile(playlist, app.outputFile);
    }
}