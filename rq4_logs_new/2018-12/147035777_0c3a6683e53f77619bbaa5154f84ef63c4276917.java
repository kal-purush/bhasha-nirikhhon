package com.example.vlad.androidapp;

import android.support.v4.app.Fragment;
import android.support.v4.app.FragmentManager;

import com.example.vlad.androidapp.Views.FavoritesFragment;
import com.example.vlad.androidapp.Views.MainFragment;
import com.example.vlad.androidapp.Views.ProductDetailFragment;
import com.example.vlad.androidapp.timer.TimerFragment;


public class NavigationManager {

    private static NavigationManager navigationManager = null;
    private FragmentManager fragmentManager;

    public void init(FragmentManager fragmentManager) {
        this.fragmentManager = fragmentManager;
    }

    public static NavigationManager getNavigationManager(){
        if (navigationManager==null){
            navigationManager = new NavigationManager();
        }
        return navigationManager;
    }

    private void open(Fragment fragment) {
        if (fragmentManager != null) {
            fragmentManager.beginTransaction()
                    .replace(R.id.container, fragment)
                    .addToBackStack(fragment.toString())
                    .commit();
        }
    }

    public void navigateBack(){
        fragmentManager.popBackStack();
    }

    public void startMain() {
        Fragment fragment = MainFragment.newInstance();
        open(fragment);
    }

    public void startProductDetail() {
        Fragment fragment = ProductDetailFragment.newInstance();
        open(fragment);
    }

    public void startFavorites() {
        Fragment fragment = FavoritesFragment.newInstance();
        open(fragment);
    }

    public void startTimer() {
        Fragment fragment = TimerFragment.newInstance();
        open(fragment);
    }
}