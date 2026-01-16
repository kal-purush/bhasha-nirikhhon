package com.talentpath.amazin.api.prime;

import javax.persistence.Entity;
import javax.persistence.Column;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.Table;


@Entity
@Table(name= "jca2", schema = "public")
public class Prime {
    @Id
//    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "itemid")
    private Long id;
    @Column(name = "prime")
    private String prime;
    @Column(name = "imgurl")
    private String imgurl;

    public Prime() {

    }

    public Prime(long id, String prime, String imgurl) {
        super();
        this.id = id;
        this.prime = prime;
        this.imgurl = imgurl;
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }
    public String getImgurl() {
        return imgurl;
    }

    public void setImgurl(String imgurl) {
        this.imgurl = imgurl;
    }

    public String getPrime() {
        return prime;
    }

    public void setPrime(String prime) {
        this.prime = prime;
    }
}