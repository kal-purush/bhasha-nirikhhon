package cz.muni.fi.pv256.movio2.uco_410034;

import android.util.SparseArray;

import cz.muni.fi.pv256.movio2.uco_410034.Model.MovieCategory;

/**
 * Created by lukas on 18.11.2017.
 */

public enum DataHolder {
    INSTANCE;

    private DataUpdateListener mDataUpdateListener;
    private SparseArray<MovieCategory> mMovieCategories = new SparseArray<>(2);

    public SparseArray<MovieCategory> getMovieCategories() {
        return mMovieCategories;
    }

    public void setMovieCategories(SparseArray<MovieCategory> movieCategories) {
        this.mMovieCategories = movieCategories;
        if(mDataUpdateListener != null) {
            mDataUpdateListener.onDataUpdate(this.mMovieCategories);
        }
    }

    public void putMovieCategory(MovieCategory movieCategory) {
        int index = mMovieCategories.indexOfValue(movieCategory);
        if(index < 0) {
            mMovieCategories.append(mMovieCategories.size(), movieCategory);
        }
        else {
            mMovieCategories.put(index, movieCategory);
        }
        if(mDataUpdateListener != null) {
            mDataUpdateListener.onDataUpdate(this.mMovieCategories);
        }
    }

    public void setDataUpdateListener(DataUpdateListener dataUpdateListener) {
        this.mDataUpdateListener = dataUpdateListener;
    }
}