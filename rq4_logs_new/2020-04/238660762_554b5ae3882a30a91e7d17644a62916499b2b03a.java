package org.styleru.mik_oil;

import android.app.Activity;
import android.os.Bundle;
import android.view.LayoutInflater;
import android.view.View;
import android.view.ViewGroup;
import android.widget.Button;
import android.widget.EditText;
import android.widget.ProgressBar;
import android.widget.TextView;
import android.widget.Toast;

import androidx.annotation.NonNull;

import com.arellomobile.mvp.MvpAppCompatFragment;
import com.arellomobile.mvp.presenter.InjectPresenter;

import java.util.Map;

import butterknife.BindView;
import butterknife.ButterKnife;
import butterknife.Unbinder;

public class PasswordRecoveryFragment extends MvpAppCompatFragment implements PasswordRecoveryView {

    @BindView(R.id.password_recovery_progressbar)
    ProgressBar progressBar;
    @BindView(R.id.password_recovery_go)
    Button goButton;
    @BindView(R.id.password_recovery_phone)
    EditText phone;
    @BindView(R.id.password_recovery_help_text)
    TextView helpText;

    private Unbinder unbinder;

    @InjectPresenter
    PasswordRecoveryPresenter presenter;

    @Override
    public View onCreateView(LayoutInflater inflater, ViewGroup container,
                             Bundle savedInstanceState) {
        return inflater.inflate(R.layout.fragment_password_recovery, container, false);
    }

    @Override
    public void onViewCreated(@NonNull View view, Bundle saveInstanceState) {
        unbinder = ButterKnife.bind(this, view);
        goButton.setOnClickListener(v ->
                presenter.onGetVerificationKeyClicked(phone.getText().toString()));
    }

    @Override
    public void onDestroyView() {
        unbinder.unbind();
        super.onDestroyView();
    }

    @Override
    public void setProgressEnabled(boolean enabled) {
        if (enabled) {
            goButton.setVisibility(View.INVISIBLE);
            progressBar.setVisibility(View.VISIBLE);
        } else {
            progressBar.setVisibility(View.GONE);
            goButton.setVisibility(View.VISIBLE);
        }
    }

    @Override
    public void showValidationErrors(Map<Field, Integer> errors) {
        for (Map.Entry<Field, Integer> error : errors.entrySet()) {
            EditText field = null;
            if (error.getKey() == Field.PHONE) {
                field = phone;
            }
            if (field != null) {
                field.setError(getString(error.getValue()));
            }
        }
    }

    @Override
    public void goToCheckingVerificationKey() {
        Activity activity = getActivity();
        if (activity != null) {
            ((FragmentNavigator) activity).goToVerificationKeyFragment();
        }
    }

    @Override
    public void showToast(String text) {
        Toast toast = Toast.makeText(getActivity(), text, Toast.LENGTH_SHORT);
        toast.show();
    }
}