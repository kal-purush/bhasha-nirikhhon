package com.example.mini_project01;

import android.graphics.Color;
import android.os.Bundle;
import android.view.LayoutInflater;
import android.view.View;
import android.view.ViewGroup;
import android.widget.TextView;

import androidx.annotation.NonNull;
import androidx.annotation.Nullable;
import androidx.fragment.app.DialogFragment;

public class UserDetailsDialog  extends DialogFragment {
    User user;

    @Override
    public void onViewCreated(@NonNull View view, @Nullable Bundle savedInstanceState) {
        TextView Firstname = view.findViewById(R.id.tvfirstname);
        TextView Lastname = view.findViewById(R.id.tvlastname);
        TextView City = view.findViewById(R.id.tvCity);

        Firstname.setText(user.getFirstname());
        Lastname.setText(user.getLastname());
        City.setText(user.getCity());

        if (user.getGender().equals("male"))
            view.setBackgroundColor(Color.parseColor("#ADD8E6"));
        else
            view.setBackgroundColor(Color.parseColor("#ffb6c1"));


    }

    @Nullable
    @Override
    public View onCreateView(@NonNull LayoutInflater inflater, @Nullable ViewGroup container, @Nullable Bundle savedInstanceState) {
        View view = inflater.inflate(R.layout.item_user_details, container, false);


        return view;

    }

    public UserDetailsDialog(User user) {
        this.user = user;
    }
}