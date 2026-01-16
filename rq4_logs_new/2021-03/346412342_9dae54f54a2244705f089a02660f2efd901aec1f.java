package com.hackner.musicompass.discogsapi.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class DiscogsMasterRelease {

    @JsonProperty("year")
    private String year;
    @JsonProperty("format")
    private List<String> format;
    @JsonProperty("master_id")
    private String masterId;
    @JsonProperty("title")
    private String fullAlbumTitle;
    @JsonProperty("community")
    private States releaseStates;
}