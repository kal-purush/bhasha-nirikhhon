package com.padsterprogramming.tracks.server;

import java.io.IOException;

/** Server entrypoint for all of Tracks. */
public class BaseServer extends com.padsterprogramming.projection.server.BaseServer {

  public static void main(String[] args) throws IOException {
    com.padsterprogramming.projection.server.BaseServer.run(new BaseServer(), args);
  }

  @Override public void configureHandlers() {
//    handleStore("/_/_/artist", new ArtistStore());
  }
}