package org.styleru.mik_oil.splashscreen;

import android.app.Activity;
import android.os.Bundle;
import android.view.LayoutInflater;
import android.view.View;
import android.view.ViewGroup;

import androidx.annotation.NonNull;

import com.arellomobile.mvp.MvpAppCompatFragment;
import com.arellomobile.mvp.presenter.InjectPresenter;

import org.styleru.mik_oil.R;
import org.styleru.mik_oil.navigation.FragmentNavigator;

public class SplashScreenFragment extends MvpAppCompatFragment implements SplashScreenView {

    @InjectPresenter
    SplashScreenPresenter presenter;

    @Override
    public View onCreateView(LayoutInflater inflater, ViewGroup container,
                             Bundle savedInstanceState) {
        return inflater.inflate(R.layout.splashscreen, container, false);
    }

    @Override
    public void onViewCreated(@NonNull View view, Bundle saveInstanceState) {
        presenter.checkRegistration();
    }

    @Override
    public void goToEntrance() {
        Activity activity = getActivity();
        if (activity != null) {
            ((FragmentNavigator) activity).goToEntranceFragment();
        }
    }

    @Override
    public void goToMain() {
    }
}