package com.macoredroid.onlinebookstore.entity;

import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

@Document(collection = "cover")
public class Cover {
    @Id
    public String bookid;

    public byte[] picture;

    public Cover(){}
    public Cover(String bookid,byte[] picture)
    {
        this.bookid=bookid;
        this.picture=picture;
    }
}