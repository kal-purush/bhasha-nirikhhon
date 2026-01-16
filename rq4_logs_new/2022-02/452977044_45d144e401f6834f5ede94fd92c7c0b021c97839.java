package com.NumCo.numberconverter.CipherCreation;

import androidx.annotation.NonNull;
import androidx.fragment.app.Fragment;
import androidx.fragment.app.FragmentActivity;
import androidx.fragment.app.FragmentManager;
import androidx.fragment.app.FragmentPagerAdapter;
import androidx.lifecycle.Lifecycle;
import androidx.viewpager2.adapter.FragmentStateAdapter;

import java.util.ArrayList;

public class CipherDialogFragmentAdapter extends FragmentStateAdapter {

    private final ArrayList<Fragment> cipherDialogFragments = new ArrayList<>();

    public CipherDialogFragmentAdapter(@NonNull FragmentManager fragmentManager, @NonNull Lifecycle lifecycle) {
        super(fragmentManager, lifecycle);
    }

    @NonNull
    @Override
    public Fragment createFragment(int position) {
        return cipherDialogFragments.get(position);
    }

    @Override
    public int getItemCount() {
        return cipherDialogFragments.size();
    }

    public void addCipherDialogFragment(Fragment dialogFragment) {
        cipherDialogFragments.add(dialogFragment);
    }
}