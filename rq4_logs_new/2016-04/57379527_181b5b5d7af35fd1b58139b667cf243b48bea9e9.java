package cnu.lecture;

import java.io.IOException;

/**
 * Hello world!
 *
 */
public class App {
    public static void main( String[] args ) throws IOException {
        final String apiKey = args[0];
        final String summonerName = args[1];

        GameParticipantListener listener = new GameParticipantListener() {
            @Override
            public void player(String summonerName) {
                System.out.println("playing summoner: " + summonerName);
            }
        };

        InGameSummonerQuerier querier = new InGameSummonerQuerier(apiKey, listener);
        querier.queryGameKey(summonerName);
    }
}