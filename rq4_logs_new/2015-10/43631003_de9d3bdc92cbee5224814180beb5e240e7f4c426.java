package com.example.android.popularmovies;

import android.os.Parcel;
import android.os.Parcelable;

import java.text.DecimalFormat;

/**
 * Created by Jake on 9/28/2015.
 */
public class MovieData implements Parcelable
{
    private String title;
    private String poster;
    private String userRating;
    private String popularity;
    private String plotOverview;
    private String background;
    private String releaseDate;

    private DecimalFormat decimalFormat = new DecimalFormat("0.00");

    public MovieData()
    {

    }

    @Override
    public int describeContents()
    {
        return 0;
    }

    @Override
    public void writeToParcel(Parcel out, int flags)
    {
        out.writeString(title);
        out.writeString(poster);
        out.writeString(userRating);
        out.writeString(popularity);
        out.writeString(plotOverview);
        out.writeString(background);
        out.writeString(releaseDate);
    }

    public void readFromParcel(Parcel in)
    {
        title = in.readString();
        poster = in.readString();
        userRating = in.readString();
        popularity = in.readString();
        plotOverview = in.readString();
        background = in.readString();
        releaseDate = in.readString();
    }

    public static final Parcelable.Creator<MovieData> CREATOR = new Parcelable.Creator<MovieData>()
    {
        public MovieData createFromParcel(Parcel in)
        {
            return new MovieData(in);
        }

        public MovieData[] newArray(int size)
        {
            return new MovieData[size];
        }
    };

    public MovieData(Parcel in)
    {
        readFromParcel(in);
    }

    public String getTitle()
    {
        return title;
    }

    public void setTitle(String title)
    {
        this.title = title;
    }

    public String getPoster()
    {
        return poster;
    }

    public void setPoster(String poster)
    {
        this.poster = "http://image.tmdb.org/t/p/w185" + poster;
    }

    public String getUserRating()
    {
        return userRating;
    }

    public void setUserRating(String userRating)
    {
        double rating = Double.parseDouble(userRating);

        this.userRating = "User Rating: " + decimalFormat.format(rating);
    }

    public String getPopularity()
    {
        return popularity;
    }

    public void setPopularity(String popularity)
    {
        this.popularity = popularity;
    }

    public String getPlotOverview()
    {
        return plotOverview;
    }

    public void setPlotOverview(String plotOverview)
    {
        this.plotOverview = plotOverview;
    }

    public String getBackground()
    {
        return background;
    }

    public void setBackground(String background)
    {
        this.background = "http://image.tmdb.org/t/p/w780" + background;
    }

    public String getReleaseDate()
    {
        return releaseDate;
    }

    public void setReleaseDate(String releaseDate)
    {
        this.releaseDate = "Released on: " + releaseDate;
    }
}