package com.cryptojobboard.cryptojobboard;

import android.os.Bundle;
import android.support.annotation.Nullable;
import android.support.v4.app.Fragment;
import android.support.v4.app.FragmentManager;
import android.support.v7.app.AppCompatActivity;

import com.cryptojobboard.cryptojobboard.feed.FeedFragment;

public class MainActivity extends AppCompatActivity {

    @Override
    protected void onCreate(@Nullable Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_main);

        loadFragment();
    }

    private static Fragment loadFragment(Fragment fragment) {
        // Create a fragment manager
        FragmentManager fm = 
        return new FeedFragment();
    }

//    private fun loadFragment(fragment: Fragment) {
//        // create a FragmentManager
//        val fm = supportFragmentManager
//        // create a FragmentTransaction to begin the transaction and replace the Fragment
//        val fragmentTransaction = fm.beginTransaction()
//        // replace the FrameLayout with new Fragment
//        fragmentTransaction.replace(R.id.frameLayout, fragment)
//        fragmentTransaction.commit() // save the changes
//    }

}