package com.NumCo.numberconverter.CipherCreation;

import android.app.Dialog;
import android.os.Bundle;
import android.view.LayoutInflater;
import android.view.View;
import android.view.ViewGroup;
import android.view.WindowManager;

import androidx.annotation.NonNull;
import androidx.annotation.Nullable;
import androidx.fragment.app.DialogFragment;
import androidx.viewpager2.widget.ViewPager2;

import com.example.numberconverter.R;
import com.google.android.material.button.MaterialButton;
import com.google.android.material.tabs.TabLayout;

import java.util.ArrayList;
import java.util.Objects;

public class CipherPreferencesDialog extends DialogFragment {
    private TabLayout tabLayout;
    private ViewPager2 viewPager;
    private MaterialButton backButton;
    @Nullable
    @Override
    public View onCreateView(@NonNull LayoutInflater inflater, @Nullable ViewGroup container, @Nullable Bundle savedInstanceState) {
        View root = inflater.inflate(R.layout.cipher_dialog_layout, container, false);
        CipherDialogFragmentAdapter cipherDialogFragmentAdapter = new CipherDialogFragmentAdapter(requireActivity(), new ArrayList<>());
        cipherDialogFragmentAdapter.addCipherDialogFragment(new CipherHelpFragment());
        cipherDialogFragmentAdapter.addCipherDialogFragment(new CipherSettingsFragment());

        tabLayout = root.findViewById(R.id.tabLayout);
        viewPager = root.findViewById(R.id.viewPager);

        backButton = root.findViewById(R.id.closeCipherPreferencesDialog);
        backButton.setText(R.string.back);

        tabLayout.addTab(tabLayout.newTab().setText(R.string.help));
        tabLayout.addTab(tabLayout.newTab().setText(R.string.settings));

        viewPager.setAdapter(cipherDialogFragmentAdapter);

        Objects.requireNonNull(getDialog()).getWindow().setBackgroundDrawableResource(R.drawable.transparent_dialog_inset_15_35);

        return root;
    }

    @Override
    public void onViewCreated(@NonNull View view, @Nullable Bundle savedInstanceState) {
        Dialog dialog = getDialog();

        if(dialog != null){
            WindowManager.LayoutParams params = dialog.getWindow().getAttributes();
            params.width = WindowManager.LayoutParams.MATCH_PARENT;

            dialog.getWindow().setAttributes(params);
        }

        setTabOnSelectedListener();
        registerPagerOnPageChangeCallback();

        backButton.setOnClickListener(v -> {
            this.dismiss();
        });
    }

    public void setTabOnSelectedListener(){
        tabLayout.addOnTabSelectedListener(new TabLayout.OnTabSelectedListener() {
            @Override
            public void onTabSelected(TabLayout.Tab tab) {
                viewPager.setCurrentItem(tab.getPosition());
            }

            @Override
            public void onTabUnselected(TabLayout.Tab tab) {

            }

            @Override
            public void onTabReselected(TabLayout.Tab tab) {

            }
        });
    }

    public void registerPagerOnPageChangeCallback(){
        viewPager.registerOnPageChangeCallback(new ViewPager2.OnPageChangeCallback() {
            @Override
            public void onPageSelected(int position) {
                tabLayout.selectTab(tabLayout.getTabAt(position));
            }
        });
    }
}