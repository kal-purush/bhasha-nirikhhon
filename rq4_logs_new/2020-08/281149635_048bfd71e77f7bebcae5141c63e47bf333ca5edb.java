package com.talentpath.amazin.api.productVariant;

import javax.persistence.*;

@Entity
@Table(name="featurev", schema = "public")
public class Feature {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name="featureid")
    private Long featureid;
    @Column(name="productvid")
    private Long productvid;
    @Column(name="title")
    private String title;
    @Column(name="rating")
    private String rating;

    public Feature() {
    }

    public Feature(Long featureid, Long productvid, String title, String rating) {
        this.featureid = featureid;
        this.productvid = productvid;
        this.title = title;
        this.rating = rating;
    }

    public Long getFeatureid() {
        return featureid;
    }

    public void setFeatureid(Long featureid) {
        this.featureid = featureid;
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public String getRating() {
        return rating;
    }

    public void setRating(String rating) {
        this.rating = rating;
    }

    public Long getProductvid() {
        return productvid;
    }

    public void setProductvid(Long productvid) {
        this.productvid = productvid;
    }
}