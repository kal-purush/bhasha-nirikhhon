package Presenters;

import android.app.Activity;
import android.content.Context;

import java.util.List;

import Model.GameOBj;

/**
 * Created by rob2cool on 10/22/15.
 */
public interface MainActivityPresenter {
    List<GameOBj> onCreateDummyList(List<GameOBj> mGames);
    void onStartRatingActivity(Activity context);
    void onStartListActivity(Activity activity);
}