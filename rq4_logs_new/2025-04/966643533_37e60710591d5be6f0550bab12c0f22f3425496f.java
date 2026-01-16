package org.spiceboys.Travel.Diary.model;

import com.fasterxml.jackson.annotation.JsonBackReference;
import jakarta.persistence.*;
import jakarta.validation.constraints.NotBlank;
import org.hibernate.validator.constraints.URL;

import javax.annotation.processing.Generated;
import java.time.LocalDateTime;

@Entity
@Table(name="photos")
public class Photo {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long photoId;

    @ManyToOne
    @JoinColumn(name = "activity_id")
    @JsonBackReference
    private Activity activity;

    private String caption;

    @URL
    @NotBlank
    private String url;

    private LocalDateTime modifiedAt;

    public Photo() {}

    public Photo(Activity activity, String caption, String url) {
        this.activity = activity;
        this.caption = caption;
        this.url = url;
        this.modifiedAt = LocalDateTime.now();
    }

    public Long getPhotoId() {
        return photoId;
    }

    public Activity getActivity() {
        return activity;
    }

    public String getCaption() {
        return caption;
    }

    public String getUrl() {
        return url;
    }

    public LocalDateTime getModifiedAt() {
        return modifiedAt;
    }

    public void setCaption(String caption) {
        this.caption = caption;
    }

    public void setModifiedAt(LocalDateTime modifiedAt) {
        this.modifiedAt = modifiedAt;
    }

    public void setUrl(String url) {
        this.url = url;
    }
}