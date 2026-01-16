package com.immortalidiot.studentapp.auth;

import android.os.Bundle;

import android.text.TextUtils;
import android.view.LayoutInflater;
import android.view.View;
import android.view.ViewGroup;
import android.widget.ProgressBar;
import android.widget.Toast;

import androidx.fragment.app.FragmentManager;

import com.google.firebase.auth.FirebaseAuth;
import com.immortalidiot.studentapp.databinding.FragmentRegistrationBinding;
import com.immortalidiot.studentapp.ui.profile.ProfileFragment;

public class RegistrationFragment extends FragmentUtils {
    FirebaseAuth auth = FirebaseAuth.getInstance();
    CallbackFragment fragment;
    FragmentRegistrationBinding binding;

    @Override
    public View onCreateView(LayoutInflater inflater, ViewGroup container,
                             Bundle savedInstanceState) {
        binding = FragmentRegistrationBinding.inflate(inflater, container, false);
        View view = binding.getRoot();
        ProgressBar progressBar = binding.progressBar;

        binding.toLogin.setOnClickListener(v -> {
            if (fragment != null) {
                closeFragment();
            }
        });

        binding.regButton.setOnClickListener(v -> {
            progressBar.setVisibility(View.VISIBLE);
            String email = String.valueOf(binding.regEmail.getText());
            String password = String.valueOf(binding.regPassword.getText());
            String passwordConfirmation = String.valueOf(binding.passwordConfirmation.getText());

            if (TextUtils.isEmpty(email)) {
                progressBar.setVisibility(View.GONE);
                Toast.makeText(getContext(),
                        "Введите почту", Toast.LENGTH_SHORT).show();
                return;
            }

            if (TextUtils.isEmpty(password)) {
                progressBar.setVisibility(View.GONE);
                Toast.makeText(getContext(),
                        "Введите пароль", Toast.LENGTH_SHORT).show();
                return;
            }

            if (TextUtils.isEmpty(passwordConfirmation)) {
                progressBar.setVisibility(View.GONE);
                Toast.makeText(getContext(),
                        "Подтвердите пароль", Toast.LENGTH_SHORT).show();
                return;
            }

            if (!password.equals(passwordConfirmation)) {
                progressBar.setVisibility(View.GONE);
                Toast.makeText(getContext(),
                        "Пароли не совпадают", Toast.LENGTH_SHORT).show();
                return;
            }

            auth.createUserWithEmailAndPassword(email, password).addOnCompleteListener(t -> {
                progressBar.setVisibility(View.GONE);
                if (fragment != null) {
                    ProfileFragment profileFragment = new ProfileFragment();
                    profileFragment.setCallbackFragment(fragment);
                    fragment.changeFragment(profileFragment, true);
                }
                // TODO: fix email verification

//                if (t.isSuccessful()) {
//                    auth.getCurrentUser().sendEmailVerification().addOnCompleteListener(task -> {
//                        if (task.isSuccessful()) {
//                            Toast.makeText(getContext(),
//                                    "Аккаунт создан, пожалуйста подтвердите почту",
//                                    Toast.LENGTH_SHORT)
//                                    .show();
//                        } else {
//                            Toast.makeText(getContext(),
//                                    "Ошибка регистрации: проверьте подключение к сети",
//                                    Toast.LENGTH_SHORT)
//                                    .show();
//                        }
//                    });
//                    if (fragment != null) {
//                        ProfileFragment profileFragment = new ProfileFragment();
//                        profileFragment.setCallbackFragment(fragment);
//                        fragment.changeFragment(profileFragment, true);
//                    }
//                } else {
//                    Toast.makeText(getContext(),
//                                    "Ошибка регистрации: попробуйте позднее",
//                                    Toast.LENGTH_SHORT)
//                            .show();
//                }
            });

        });


        return view;
    }

    public void setCallbackFragment(CallbackFragment fragment) {
        this.fragment = fragment;
    }
    private void closeFragment() {
        FragmentManager fragmentManager = getParentFragmentManager();
        fragmentManager.popBackStack();
    }
}