package myapp;

/**
 * @author Artem Grebenkin <ag@nemeses.de>
 */
public class Channel {
    private String token;

    public Channel() {
    }

    public Channel(final String token) {

        this.token = token;
    }

    public String getToken() {
        return token;
    }

    public void setToken(final String token) {
        this.token = token;
    }

    @Override public String toString() {
        return "Channel{" +
                "token='" + token + '\'' +
                '}';
    }
}
