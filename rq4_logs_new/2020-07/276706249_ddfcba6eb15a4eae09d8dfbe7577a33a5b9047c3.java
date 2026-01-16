package com.dtavana.foodswipe.utils;

import android.content.Context;
import android.view.MenuItem;
import android.widget.Toast;

import androidx.annotation.NonNull;
import androidx.fragment.app.Fragment;
import androidx.fragment.app.FragmentManager;

import com.dtavana.foodswipe.R;
import com.dtavana.foodswipe.databinding.ActivityMainBinding;
import com.google.android.material.bottomnavigation.BottomNavigationView;

public class SetupNavigation {
    public static void run(final FragmentManager manager , final Context ctx, ActivityMainBinding binding) {
        binding.navigation.getRoot().setOnNavigationItemSelectedListener(new BottomNavigationView.OnNavigationItemSelectedListener() {
            @Override
            public boolean onNavigationItemSelected(@NonNull MenuItem item) {
                Fragment fragment;
                switch (item.getItemId()) {
                    case R.id.miList:
                        Toast.makeText(ctx, "List Restaurants", Toast.LENGTH_SHORT).show();
                        break;
                    case R.id.miCycle:
                        Toast.makeText(ctx, "Cycle Restaurants", Toast.LENGTH_SHORT).show();
                        break;
                    case R.id.miProfile:
                    default:
                        Toast.makeText(ctx, "Profile", Toast.LENGTH_SHORT).show();
                        break;
                }
//                manager.beginTransaction().replace(R.id.flContainer, fragment).commit();
                return true;
            }
        });
    }
}