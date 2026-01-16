package tictactoe.domain.usecases;

import javax.json.Json;

public class ToJsonUseCase {

    public static String toJson(String email, String password) {
        return Json.createObjectBuilder().
                add("action", 2)
                .add("email", email)
                .add("password", password)
                .build().
                toString();

    }

    public static String toJsonScoreUpdate(String email, int score) {
        return Json.createObjectBuilder()
                .add("action", 7)
                .add("username", email)
                .add("score", score)
                .build()
                .toString();
    }

    public static String updateIsAvailable(String playerName) {
        return Json.createObjectBuilder()
                .add("action", 8)
                .add("username", playerName)
                .build()
                .toString();
    }

    public static String toJson(int action, String username1, String username2, int score1, int score2, int status) {
        return Json.createObjectBuilder()
                .add("action", action)
                .add("username-player1", username1)
                .add("username-player2", username2)
                .add("score-player1", score1)
                .add("score-player2", score2)
                .add("status", status)
                .build().toString();
    }

}