package com.jefftiensivu.popularmovies.data;

import net.simonvt.schematic.annotation.AutoIncrement;
import net.simonvt.schematic.annotation.DataType;
import net.simonvt.schematic.annotation.NotNull;
import net.simonvt.schematic.annotation.PrimaryKey;

/**
 * Created by jeff on 10/31/2015.
 */
public interface PopMoviesColumns {

    @DataType(DataType.Type.INTEGER) @PrimaryKey
    @AutoIncrement
    String _ID = "_id";

    @DataType(DataType.Type.INTEGER) @NotNull
    String TMDB_ID = "tmdb_id";

    @DataType(DataType.Type.TEXT) @NotNull
    String TITLE = "title";

    @DataType(DataType.Type.TEXT)
    String OVERVIEW = "overview";

    @DataType(DataType.Type.TEXT)
    String RELEASE_DATE = "release_date";

    @DataType(DataType.Type.TEXT)
    String POSTER_PATH = "poster_path";

    @DataType(DataType.Type.REAL) @NotNull
    String POPULARITY = "popularity";

    @DataType(DataType.Type.REAL) @NotNull
    String VOTE_AVERAGE = "vote_average";

    @DataType(DataType.Type.INTEGER) @NotNull
    String VOTE_COUNT = "vote_count";

    @DataType(DataType.Type.INTEGER)
    String RUN_TIME = "run_time";

    @DataType(DataType.Type.INTEGER)
    String FAV = "fav";

    @DataType(DataType.Type.INTEGER)
    String TIME_STAMP = "time_stamp";

    @DataType(DataType.Type.TEXT)
    String JPEG = "jpeg";

/*
    @DataType(class model.Trailers)
    public static final String TRAILERS = "trailers";

    @DataType(class model.Reviews)
    public static final String REVIEWS = "reviews";
*/
}