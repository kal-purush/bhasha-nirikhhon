package com.web.crawler.model;

import java.util.Objects;

public final class CrawledLink {

    private final String crawledFullLink;
    private final String crawledLink;

    public CrawledLink(String crawledFullLink, String crawledLink) {
        this.crawledFullLink = crawledFullLink;
        this.crawledLink = crawledLink;
    }

    public String getCrawledFullLink() {
        return crawledFullLink;
    }

    public String getCrawledLink() {
        return crawledLink;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof CrawledLink)) return false;
        CrawledLink that = (CrawledLink) o;
        return Objects.equals(crawledFullLink, that.crawledFullLink) &&
                Objects.equals(crawledLink, that.crawledLink);
    }

    @Override
    public int hashCode() {

        return Objects.hash(crawledFullLink, crawledLink);
    }
}