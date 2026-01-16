package es.travelworld.traveling;

import android.os.Bundle;
import android.view.LayoutInflater;
import android.view.View;
import android.view.ViewGroup;

import androidx.annotation.NonNull;
import androidx.annotation.Nullable;
import androidx.fragment.app.Fragment;
import androidx.navigation.Navigation;
import es.travelworld.traveling.databinding.OnboardingFragmentBinding;

public class OnBoardingFragment extends Fragment {

    private OnboardingFragmentBinding binding;

    @Override
    public View onCreateView(@NonNull LayoutInflater inflater, ViewGroup container,
                             Bundle savedInstanceState) {
       binding = OnboardingFragmentBinding.inflate(inflater, container, false);
       return binding.getRoot();
    }

    @Override
    public void onViewCreated(@NonNull View view, @Nullable Bundle savedInstanceState) {
        super.onViewCreated(view, savedInstanceState);
        setListeners();
    }

    private void setListeners() {
        binding.textViewNext.setOnClickListener(view -> Navigation.findNavController(view).navigate(R.id.action_onboardingFragment_to_homeFragment));
    }
}