package cz.muni.fi.pv256.movio2.uco_410034.Api;

import java.util.List;

import cz.muni.fi.pv256.movio2.uco_410034.Api.Model.DiscoverMovies;
import cz.muni.fi.pv256.movio2.uco_410034.Api.Model.Movie;
import retrofit2.Call;
import retrofit2.http.GET;
import retrofit2.http.Query;

/**
 * Created by lukas on 25.11.2017.
 */

public interface MovieAPI {

    @GET("discover/movie")
    Call<DiscoverMovies> discoverMovies(@Query("api_key") String apiKey,
                                        @Query("sort_by") String sortBy,
                                        @Query("include_adult") String includeAdult,
                                        @Query("include_video") String includeVideo,
                                        @Query("language") String language,
                                        @Query("primary_release_date.gte") String releaseDateFrom,
                                        @Query("primary_release_date.lte") String releaseDateTo,
                                        @Query("certification_country") String certificationCountry,
                                        @Query("certification") String certification);
}