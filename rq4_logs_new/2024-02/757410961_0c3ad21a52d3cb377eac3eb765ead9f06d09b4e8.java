package edu.java.bot.links;

import edu.java.bot.utils.Link;
import java.net.URI;

public class GitHubHandler extends LinkHandler {
    private final static String host = LinkInfo.GITHUB.getHost();

    @Override
    public Link handleRequest(String url) {
        if (url.contains(host)) {
            System.out.println("Отслеживание обновлений на GitHub: " + url);
            return new Link(URI.create(url), LinkInfo.GITHUB, host);
        } else {
            return super.handleRequest(url);
        }
    }
}