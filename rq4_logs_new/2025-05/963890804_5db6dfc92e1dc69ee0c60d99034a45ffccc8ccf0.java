package com.example.MusicApp.DTO;

public class SongDTO {
    private Long id;
    private String title;
    private String artist;
    private String fileUrl;
    private String imageUrl;
    private String lyrics;
    private int likes;
    private int dislikes;
    private int views;

    public SongDTO(Long id, String title, String artist, String fileUrl, String imageUrl, String lyrics, int likes, int dislikes, int views) {
        this.id = id;
        this.title = title;
        this.artist = artist;
        this.fileUrl = fileUrl;
        this.imageUrl = imageUrl;
        this.lyrics = lyrics;
        this.likes = likes;
        this.dislikes = dislikes;
        this.views = views;
    }

    public Long getId() { return id; }
    public String getTitle() { return title; }
    public String getArtist() { return artist; }
    public String getFileUrl() { return fileUrl; }
    public String getImageUrl() { return imageUrl; }
    public String getLyrics() { return lyrics; }
    public int getLikes() { return likes; }
    public int getDislikes() { return dislikes; }
    public int getViews() { return views; }
}
