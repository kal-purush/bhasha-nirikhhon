package eu.javeo.knowler.client.mobile.knowler.model;

import android.util.SparseArray;

import java.util.Date;

/**
 * Created by rjuzaszek on 23.07.15.
 */
public class Lecture {
    //dane
    private String videoUrl;
    private String title;
    private Date createdDate;
    private String filename;

    public SparseArray<SlideElement> mapping;

    //konstruktory
    public Lecture() {
        videoUrl = null;
        title = null;
        createdDate = null;
        filename = null;
        mapping = null;
    }

    public Lecture(String videoUrl, String title, Date createdDate, String filename) {
        this.videoUrl = videoUrl;
        this.title = title;
        this.createdDate = createdDate;
        this.filename = filename;
    }

    public Lecture(Lecture lec) {
        this.videoUrl = lec.getVideoUrl();
        this.title = lec.getTitle();
        this.createdDate = lec.getCreatedDate();
        this.filename = lec.getFilename();

        //mapping = new SparseArray<SlideElement>();
        //for(int i=0; i < lec.mapping.size(); i++) {
         //   mapping.append(lec.mapping.keyAt(i),new );
            //musze sprawdzić czy metoda clone działa do głebokiego kopiowania całego SparseArray
       // }
    }

    //metody dostępowe
    String getVideoUrl() {
        return videoUrl;
    }
    String getTitle() {
        return title;
    }
    Date getCreatedDate() {
        return createdDate;
    }
    String getFilename() {
        return filename;
    }

    void setVideoUrl(String videoUrl) {
        this.videoUrl = videoUrl;
    }
    void setTitle(String title) {
        this.title = title;
    }
    void setCreatedDate(Date createdDate) {
        this.createdDate = createdDate;
    }
    void setFilename(String filename) {
        this.filename = filename;
    }
}