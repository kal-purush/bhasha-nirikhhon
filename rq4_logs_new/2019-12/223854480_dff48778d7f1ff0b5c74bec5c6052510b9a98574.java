package com.blueroofstudio.instagramclient.fragments.fruits;

import android.os.Bundle;
import android.view.LayoutInflater;
import android.view.View;
import android.view.ViewGroup;
import android.widget.ImageView;

import androidx.annotation.NonNull;
import androidx.annotation.Nullable;
import androidx.fragment.app.Fragment;
import androidx.lifecycle.Observer;
import androidx.lifecycle.ViewModelProviders;

import com.blueroofstudio.instagramclient.R;

public class FruitsFragment extends Fragment {

    private FruitsViewModel fruitsViewModel;

    public View onCreateView(@NonNull LayoutInflater inflater,
                             ViewGroup container, Bundle savedInstanceState) {
        fruitsViewModel =
                ViewModelProviders.of(this).get(FruitsViewModel.class);
        View root = inflater.inflate(R.layout.fragment_image, container, false);

        final ImageView imageView = root.findViewById(R.id.imageView);
        fruitsViewModel.getDrawableResource().observe(this, new Observer<Integer>() {
            @Override
            public void onChanged(@Nullable Integer s) {
                imageView.setImageResource(s);
            }
        });
        return root;
    }
}