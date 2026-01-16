package info.santhosh.evlo.ui.favorites;

import android.os.Bundle;
import android.support.v7.app.ActionBar;
import android.support.v7.app.AppCompatActivity;
import android.support.v7.widget.Toolbar;

import info.santhosh.evlo.R;

/**
 * Created by santhoshvai on 10/12/2016.
 */

public class FavoritesActivity extends AppCompatActivity {
    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_favorites);
        Toolbar toolbar = (Toolbar) findViewById(R.id.fav_toolbar);
        setSupportActionBar(toolbar);

        // Show the Up button in the action bar.
        ActionBar actionBar = getSupportActionBar();
        if (actionBar != null) {
            actionBar.setDisplayHomeAsUpEnabled(true);
        }

        if (savedInstanceState == null) {
            FavoritesFragment favoritesFragment = FavoritesFragment.newInstance();
            getSupportFragmentManager().beginTransaction()
                    .add(R.id.favs_fragment_container, favoritesFragment)
                    .commit();
        }

        // we set the background for viewgroup in xml, no need for window background
        getWindow().setBackgroundDrawable(null);
    }
}