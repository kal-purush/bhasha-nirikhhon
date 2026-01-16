package com.rtaborda.popularmoviesapp.data;

import net.simonvt.schematic.annotation.Database;
import net.simonvt.schematic.annotation.Table;

/**
 * Created by Rui on 21/09/2015.
 */
@Database(version = FavouriteDatabase.VERSION)
public final class FavouriteDatabase {

    public static final int VERSION = 1;

    @Table(FavouriteTitleColumns.class)
    public static final String FAVOURITES = "favourites";

    @Table(FavouriteTitleColumns.class)
    public static final String REVIEWS = "reviews";

    @Table(FavouriteTitleColumns.class)
    public static final String VIDEOS = "videos";
}