package com.happiday.Happi_Day.utils;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
public class DefaultImageUtils {

    @Value("${app.default-image-url.article-thumbnail}")
    private String defaultImageUrlArticle;
    @Value("${app.default-image-url.event-thumbnail}")
    private String defaultImageUrlEvent;
    @Value("${app.default-image-url.sales-thumbnail}")
    private String defaultImageUrlSales;
    @Value("${app.default-image-url.user-profile}")
    private String defaultImageUrlUser;
    @Value("${app.default-image-url.team-artist-profile}")
    private String defaultImageUrlTeamArtist;

    public String getDefaultImageUrlArticleThumbnail() {
        return defaultImageUrlArticle;
    }

    public String getDefaultImageUrlEventThumbnail() {
        return defaultImageUrlEvent;
    }

    public String getDefaultImageUrlSalesThumbnail() {
        return defaultImageUrlSales;
    }

    public String getDefaultImageUrlUserProfile() {
        return defaultImageUrlUser;
    }

    public String getDefaultImageUrlTeamArtistProfile() {
        return defaultImageUrlTeamArtist;
    }
}