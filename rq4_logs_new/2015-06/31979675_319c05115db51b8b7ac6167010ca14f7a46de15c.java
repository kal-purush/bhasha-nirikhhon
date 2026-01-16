package androidpath.ll.material.Utils;

import com.activeandroid.ActiveAndroid;
import com.activeandroid.query.Select;

import java.util.ArrayList;
import java.util.List;

import androidpath.ll.material.Models.Movie;

/**
 * Created by Le on 2015/6/27.
 */
public class DBHelper {
    public static String CATEGORY_BOXOFFICE = "boxoffice";

    public void insertMoviesBoxOffice(ArrayList<Movie> listMovies, boolean clearPrevious) {
        if (clearPrevious) {
            Movie.clearData();
        }

        //Transaction will speed up the process
        ActiveAndroid.beginTransaction();
        try {
            for (Movie currentMovie : listMovies) {
                currentMovie.save();
            }
            ActiveAndroid.setTransactionSuccessful();
        } finally {
            ActiveAndroid.endTransaction();
        }
    }

    public List<Movie> getAll() {
        // This is how you execute a query
        return new Select()
                .from(Movie.class)
                .execute();
    }


}