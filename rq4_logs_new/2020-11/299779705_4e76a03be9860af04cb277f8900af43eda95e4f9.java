package com.example.cmput301f20t18;

import android.os.Bundle;

import androidx.annotation.NonNull;
import androidx.fragment.app.Fragment;
import androidx.fragment.app.FragmentManager;
import androidx.fragment.app.FragmentPagerAdapter;

//This needs to be where ever a cover book is at
/*                mPager = findViewById(R.id.slider_viewer);
                imageAdapter = new ImagePageAdapter (getSupportFragmentManager(),1);
                mPager.setAdapter(imageAdapter);    */


public class ImagePageAdapter extends FragmentPagerAdapter {

   //This is where the images of a book would be at

    public ImagePageAdapter(@NonNull FragmentManager fm, int behavior) {
        super(fm, behavior);
    }

    @NonNull
    @Override
    public Fragment getItem(int position) {

        ImageSliderFragment imgFrag = new ImageSliderFragment();
        //This is where the images would be bundle for the image slider fragment
        /*
        Bundle imgBundle = new Bundle();
        imgBundle.putString("Source",imageURLs[position]);
        imgFrag.setArguments(imgBundle);*/

        return imgFrag;
    }

    @Override
    public int getCount() {
        return 0; /*this would be the length of the image string array*/
    }
}

/* <?xml version="1.0" encoding="utf-8"?>

Also their needs to be a ViewPager layout like so...

<androidx.constraintlayout.widget.ConstraintLayout
    xmlns:android="http://schemas.android.com/apk/res/android"
    xmlns:app="http://schemas.android.com/apk/res-auto"
    xmlns:tools="http://schemas.android.com/tools"
    android:layout_width="match_parent"
    android:layout_height="match_parent"
    tools:context=".MainActivity">

    <androidx.viewpager.widget.ViewPager
        android:id="@+id/slider_viewer"
        android:layout_width="411dp"
        android:layout_height="667dp"
        app:layout_constraintStart_toStartOf="parent"
        app:layout_constraintTop_toTopOf="parent" />

</androidx.constraintlayout.widget.ConstraintLayout>*/