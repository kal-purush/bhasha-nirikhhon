package com.example.geekbrainsandroidweather.fragments;

import android.os.Bundle;
import android.view.KeyEvent;
import android.view.LayoutInflater;
import android.view.View;
import android.view.ViewGroup;
import android.view.inputmethod.EditorInfo;
import android.widget.Button;
import android.widget.EditText;
import android.widget.TextView;

import androidx.annotation.NonNull;
import androidx.annotation.Nullable;
import androidx.fragment.app.Fragment;
import androidx.recyclerview.widget.DividerItemDecoration;
import androidx.recyclerview.widget.LinearLayoutManager;
import androidx.recyclerview.widget.RecyclerView;

import com.example.geekbrainsandroidweather.R;
import com.example.geekbrainsandroidweather.recycler_views.IRVOnItemClick;
import com.example.geekbrainsandroidweather.recycler_views.RecyclerDataAdapter;
import com.google.android.material.snackbar.Snackbar;
import com.google.android.material.textfield.TextInputEditText;

import static com.example.geekbrainsandroidweather.fragments.CitiesFragment.cities;

public class AddCityFragment extends Fragment implements IRVOnItemClick {
    private TextInputEditText addCityInput;
    private RecyclerView recyclerCitiesView;
    private RecyclerDataAdapter recyclerDataAdapter;

    @Override
    public View onCreateView(LayoutInflater inflater, ViewGroup container,
                             Bundle savedInstanceState) {
        return inflater.inflate(R.layout.fragment_add_city, container, false);
    }

    @Override
    public void onViewCreated(@NonNull View view, @Nullable Bundle savedInstanceState) {
        super.onViewCreated(view, savedInstanceState);
        initViews(view);
        setupRecyclerView();
        addCityInput.setOnEditorActionListener(new EditText.OnEditorActionListener() {
                    @Override
                    public boolean onEditorAction(TextView v, int actionId, KeyEvent event) {
                        if (actionId == EditorInfo.IME_ACTION_NEXT) {
                            String inputText = addCityInput.getText().toString();
                            if (!inputText.equals("")) {
                                if (!cities.contains(inputText)) {
                                    cities.add(inputText);
                                    setupRecyclerView();
                                    addCityInput.getText().clear();
                                } else {
                                    Snackbar.make(view, R.string.snackbar_sity_already_added, Snackbar.LENGTH_LONG).setAnchorView(addCityInput).show();
                                }
                            }
                        }
                        return true;
                    }
                });
    }

    private void initViews(View view) {
        addCityInput = view.findViewById(R.id.addCityInput);
        recyclerCitiesView = view.findViewById(R.id.recyclerCitiesView);
    }


    private void setupRecyclerView() {
        LinearLayoutManager linearLayoutManager = new LinearLayoutManager(getContext());
        DividerItemDecoration decorator = new DividerItemDecoration(getContext(),
                LinearLayoutManager.VERTICAL);
        recyclerDataAdapter = new RecyclerDataAdapter(cities, this, this);
        recyclerCitiesView.setLayoutManager(linearLayoutManager);
        recyclerCitiesView.addItemDecoration(decorator);
        recyclerCitiesView.setAdapter(recyclerDataAdapter);
    }

    @Override
    public void onItemClicked(View view, int position) {

    }

    @Override
    public void onItemLongPressed(View view) {
        TextView textView = (TextView) view;
        deleteItem(textView);
    }

    private void deleteItem(TextView view) {
        Snackbar.make(view, R.string.snackbar_delete_city, Snackbar.LENGTH_LONG)
                .setAction(R.string.apply_delete, new View.OnClickListener() {
                    @Override
                    public void onClick(View v) {
                        String cityName = view.getText().toString();
                        recyclerDataAdapter.remove(cityName);
                        cities.remove(cityName);
                    }
                }).show();
    }

    @Override
    public void changeItem(TextView view) {

    }
}