package com.imibragimov.loftmoney;

import android.app.Activity;
import android.content.Intent;
import android.os.Bundle;
import android.view.LayoutInflater;
import android.view.View;
import android.view.ViewGroup;
import android.widget.Button;

import androidx.annotation.NonNull;
import androidx.annotation.Nullable;
import androidx.fragment.app.Fragment;
import androidx.recyclerview.widget.DividerItemDecoration;
import androidx.recyclerview.widget.LinearLayoutManager;
import androidx.recyclerview.widget.RecyclerView;

import com.google.android.material.floatingactionbutton.FloatingActionButton;

import java.util.ArrayList;
import java.util.List;

public class BudgetFragment extends Fragment {

    public static final int REQUEST_CODE = 100;
    private static final String ARG_CURRENT_POSITION = "position";
    public static final String ARG_BUDGET = "arg_budget";
    public static final String ARG_VALUE = "arg_value";
    private final ItemsAdapter itemsAdapter = new ItemsAdapter();
    private final List<Item> itemList = new ArrayList<>();
    private int currentPosition;

    @Override
    public void onCreate(@Nullable Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        if (getArguments() != null ) {
            currentPosition = getArguments().getInt(ARG_CURRENT_POSITION);
        }
    }

    @Nullable
    @Override
    public View onCreateView(@NonNull LayoutInflater inflater, @Nullable ViewGroup container, @Nullable Bundle savedInstanceState) {

        View view = inflater.inflate(R.layout.fragment_budget, null);

        FloatingActionButton callAddButton = view.findViewById(R.id.call_add_item_activity);
        callAddButton.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(final View v) {
                startActivityForResult(new Intent(getActivity(), AddItemActivity.class),
                        REQUEST_CODE);
            }
        });

        RecyclerView recyclerView = view.findViewById(R.id.budget_item_list);
        recyclerView.setAdapter(itemsAdapter);

        LinearLayoutManager layoutManager = new LinearLayoutManager(getActivity(), LinearLayoutManager.VERTICAL, false);
        recyclerView.setLayoutManager(layoutManager);

        DividerItemDecoration dividerItemDecoration = new DividerItemDecoration(recyclerView.getContext(),
                layoutManager.getOrientation());

        recyclerView.addItemDecoration(dividerItemDecoration);
        recyclerView.setLayoutManager(layoutManager);

        return view;
    }

    @Override
    public void onActivityResult(
            final int requestCode, final int resultCode, @Nullable final Intent data
    ) {
        super.onActivityResult(requestCode, resultCode, data);

        String name = data.getStringExtra(ARG_BUDGET);
        String price = data.getStringExtra(ARG_VALUE);
        itemList.add(new Item(name, price, currentPosition));
        itemsAdapter.setData(itemList);

        //mAdapter.addItem(new Item(data.getStringExtra("name"), priceAdd, "typeBudget"));
    }
    public static BudgetFragment newInstance(int position) {
        BudgetFragment fragment = new BudgetFragment();
        Bundle args = new Bundle();
        args.putInt(ARG_CURRENT_POSITION, position);
        fragment.setArguments(args);
        return fragment;
    }
}

//информация хранится в ресурсе arrays.xml
//String[] names = getResources().getStringArray(R.array.expenses_name);
//int[] prices = getResources().getIntArray(R.array.expenses_value);
//for (int i = 0; i < names.length; i++) {
//    mAdapter.addItem(new Item(names[i], prices[i]));
//}






