package com.web.crawler.crawling.builder;

import com.web.crawler.normalizer.UrlNormalizer;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class FullLinkBuilder {
//TODO this is proof of concept -- test it
    private static final String CHECK_LINK_REGEX = "www(.*)|https(.*)|http(.*)";
    private static final String MAIN_LINK_REGEX = "(.*)\\.pl|(.*)\\.org|(.*)\\.com";

    public String checkLinks(String url, String link){

        Pattern p = Pattern.compile(CHECK_LINK_REGEX);
        Matcher m = p.matcher(link);
        url = extractMainLink(url);

        if (m.find()) {
            return new UrlNormalizer().normalize(link);
        }

        return validateLinkEnd(url) + validateLinkStart(link);
    }
    private String extractMainLink(String url) {

        Pattern p = Pattern.compile(MAIN_LINK_REGEX);
        Matcher m = p.matcher(url);

        if(m.find())
            return m.group(0);

        return url;
    }

    private String validateLinkStart(String url) {

        if (!url.startsWith("/")) {
            return validateLinkEnd("/" + url);
        }
        return validateLinkEnd(url);
    }

    private String validateLinkEnd(String url) {

        if (url.endsWith("/")) {
            return url.substring(0, url.length() - 1);
        }
        return url;
    }
}