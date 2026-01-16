package com.example.a327finalprojectcalorietracker.ui.dashboard;

import android.os.Bundle;
import android.view.LayoutInflater;
import android.view.View;
import android.view.ViewGroup;
import android.widget.ProgressBar;
import android.widget.TextView;

import androidx.annotation.NonNull;
import androidx.fragment.app.Fragment;
import androidx.lifecycle.ViewModelProvider;

import com.example.a327finalprojectcalorietracker.R;
import com.example.a327finalprojectcalorietracker.databinding.FragmentDashboardBinding;
import androidx.annotation.NonNull;
import androidx.fragment.app.Fragment;
import androidx.lifecycle.ViewModelProvider;

import com.example.a327finalprojectcalorietracker.Add_Meal;
import com.example.a327finalprojectcalorietracker.R;
import com.example.a327finalprojectcalorietracker.databinding.FragmentHomeBinding;import android.os.Handler;
import android.os.Looper;
public class DashboardFragment extends Fragment {

    private FragmentDashboardBinding binding;
    private ProgressBar progressBar;
    private Handler handler;
    private TextView foodCal;
    private TextView budgetCal;
    private TextView remainingCal;

    public View onCreateView(@NonNull LayoutInflater inflater,
                             ViewGroup container, Bundle savedInstanceState) {
        DashboardViewModel dashboardViewModel =
                new ViewModelProvider(this).get(DashboardViewModel.class);

        binding = FragmentDashboardBinding.inflate(inflater, container, false);
        View root = binding.getRoot();


        foodCal = root.findViewById(R.id.foodCal);
        budgetCal = root.findViewById(R.id.budgetCal);
        remainingCal = root.findViewById(R.id.remainingCal);




        final TextView textView = binding.textDashboard;
        dashboardViewModel.getText().observe(getViewLifecycleOwner(), textView::setText);


        handler = new Handler(Looper.getMainLooper());
        progressBar = root.findViewById(R.id.progressBar);
        int initialProgress = 0;
        updateProgress(initialProgress, 1);

        startUpdatingProgress();



        return root;
    }




    private int getFoodCal(int addCal, int currentCal){
        return addCal + currentCal;
    }
    private int getRemainingCal(int foodIn) {
        return 2000 - foodIn;
    }
    private void startUpdatingProgress() {

        long delayMillis = 500;

        Runnable updateProgressRunnable = new Runnable() {
            @Override
            public void run() {
                // need to update with proper logic


                int foodCalVal = 0;

                foodCal.setText(String.valueOf(foodCalVal));

                int budgetCalVal = 0;
                budgetCal.setText(String.valueOf(budgetCalVal));

                int remainingCalVal = getRemainingCal(foodCalVal);
                remainingCal.setText(String.valueOf(remainingCalVal));

                int updatedValue = foodCalVal;
                int maxCal = getMaxCal();

                // Update progress based on the new value
                updateProgress(updatedValue, maxCal);

                handler.postDelayed(this, delayMillis);
            }
        };

        handler.postDelayed(updateProgressRunnable, delayMillis);
    }


    private int getMaxCal()
    {
        return 2000;
    }

    private void updateProgress(int anotherValue, int maxInput) {
        // Calculate progress based on another value , needs adjustment
        int progress = (int) ((float) anotherValue / maxInput  * 100);

        // Set the progress to the ProgressBar
        progressBar.setProgress(progress);
    }

    @Override
    public void onDestroyView() {
        super.onDestroyView();
        binding = null;
    }
}