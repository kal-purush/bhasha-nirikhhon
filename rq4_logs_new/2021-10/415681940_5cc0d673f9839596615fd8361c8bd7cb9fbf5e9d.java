package com.example.songr.models;

public class SongDTO {
    private String title;
    private int length;
    private Long trackNumber;
    private String albumTitle;

    public SongDTO() {
    }

    public SongDTO(String title, int length, Long trackNumber, String albumTitle) {
        this.title = title;
        this.length = length;
        this.trackNumber = trackNumber;
        this.albumTitle = albumTitle;
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public int getLength() {
        return length;
    }

    public void setLength(int length) {
        this.length = length;
    }

    public Long getTrackNumber() {
        return trackNumber;
    }

    public void setTrackNumber(Long trackNumber) {
        this.trackNumber = trackNumber;
    }

    public String getAlbumTitle() {
        return albumTitle;
    }

    public void setAlbumTitle(String albumTitle) {
        this.albumTitle = albumTitle;
    }
}