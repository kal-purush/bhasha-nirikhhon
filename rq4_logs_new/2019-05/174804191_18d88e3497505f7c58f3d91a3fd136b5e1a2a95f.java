package ru.sstu.se20.DTO;

import ru.sstu.se20.DAL.Entities.ImageEmotionsEntity;

import java.util.Collection;
import java.util.Date;

public class YourArtDTO {

    public YourArtDTO() {

    }

    public YourArtDTO(int id, byte privateStatus, Date creationDate, String originalImages, String contextImages, String resultImage) {
        this.id = id;
        this.privateStatus = privateStatus;
        this.creationDate = creationDate;
        this.originalImage = originalImages;
        this.contextImage = contextImages;
        this.resultImage = resultImage;
    }

    public int id;
    public byte privateStatus;
    public java.util.Date creationDate;
    public String originalImage;
    public String contextImage;
    public String resultImage;
//    private Collection<ImageEmotionsEntity> imageEmotionsById;
//    private UsersEntity usersByUser;
}