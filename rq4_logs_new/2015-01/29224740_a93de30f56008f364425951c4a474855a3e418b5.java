package com.juangdiaz.bookshelf.model;

import android.os.Parcel;
import android.os.Parcelable;


/**
 * Created by juangdiaz on 1/13/15.
 * Implementing Parcelable which is more efficient/fast than *
 * Serializable                                              *
 * http://www.developerphil.com/parcelable-vs-serializable/  *
 */
public class Book implements Parcelable {

    public Book(){}

    private int id;
    private String author;
    private String categories;
    private String lastCheckedOut;
    private String lastCheckedOutBy;
    private String publisher;
    private String title;
    private String url;

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getAuthor() {
        return author;
    }

    public void setAuthor(String author) {
        this.author = author;
    }

    public String getCategories() {
        return categories;
    }

    public void setCategories(String categories) {
        this.categories = categories;
    }

    public String getLastCheckedOut() {
        return lastCheckedOut;
    }

    public void setLastCheckedOut(String lastCheckedOut) {
        this.lastCheckedOut = lastCheckedOut;
    }

    public String getLastCheckedOutBy() {
        return lastCheckedOutBy;
    }

    public void setLastCheckedOutBy(String lastCheckedOutBy) {
        this.lastCheckedOutBy = lastCheckedOutBy;
    }

    public String getPublisher() {
        return publisher;
    }

    public void setPublisher(String publisher) {
        this.publisher = publisher;
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }



    @Override
    public int describeContents() {
        return 0;
    }

    @Override
    public void writeToParcel(Parcel dest, int flags) {
        dest.writeInt(id);
        dest.writeString(author);
        dest.writeString(categories);
        dest.writeString(lastCheckedOut);
        dest.writeString(lastCheckedOutBy);
        dest.writeString(publisher);
        dest.writeString(title);
        dest.writeString(url);

    }





    public static final Parcelable.Creator<Book> CREATOR
            = new Parcelable.Creator<Book>() {
        public Book createFromParcel(Parcel in) {
            return new Book(in);
        }

        public Book[] newArray(int size) {
            return new Book[size];
        }
    };

    private Book(Parcel in) {
        id = in.readInt();
        author = in.readString();
        categories = in.readString();
        lastCheckedOut = in.readString();
        lastCheckedOutBy = in.readString();
        publisher = in.readString();
        title = in.readString();
        url = in.readString();
    }
    
    
}