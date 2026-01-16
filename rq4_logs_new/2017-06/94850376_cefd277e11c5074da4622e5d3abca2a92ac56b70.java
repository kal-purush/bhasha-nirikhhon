package com.example.mitya.loftmoneytracker;

import android.os.Bundle;
import android.support.v7.app.AppCompatActivity;
import android.support.v7.widget.RecyclerView;
import android.view.LayoutInflater;
import android.view.View;
import android.view.ViewGroup;
import android.widget.TextView;

import java.util.ArrayList;
import java.util.List;


public class TempActivity extends AppCompatActivity {

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.items);
        final RecyclerView items = (RecyclerView) findViewById(R.id.items);
        items.setAdapter(new ItemsAdapter());
    }

    @Override
    protected void onResume() {
        super.onResume();
    }

    @Override
    protected void onStart() {
        super.onStart();
    }

    private class ItemsAdapter extends RecyclerView.Adapter<ItemViewHolder> {
        private final List<Item> items = new ArrayList<>();

        ItemsAdapter() {
            items.add(new Item("car", 100));
            items.add(new Item("apple", 400));
            items.add(new Item("car", 100));
            items.add(new Item("apple", 400));
            items.add(new Item("car", 100));
            items.add(new Item("apple", 400));
        }

        @Override
        public ItemViewHolder onCreateViewHolder(ViewGroup parent, int viewType) {
            return new ItemViewHolder(LayoutInflater.from(parent.getContext()).inflate(R.layout.item, null));
        }

        @Override
        public void onBindViewHolder(ItemViewHolder holder, int position) {
            Item item = items.get(position);
            holder.name.setText(item.name);
            holder.price.setText(String.valueOf(item.price));

        }

        @Override
        public int getItemCount() {
            return items.size();
        }

        void add(Item item) {
            items.add(0, item);
            notifyItemInserted(0);
        }


    }

    static class ItemViewHolder extends RecyclerView.ViewHolder {
        private final TextView name, price;

        ItemViewHolder(View itemView) {
            super(itemView);
            name = (TextView) itemView.findViewById(R.id.name);
            price = (TextView) itemView.findViewById(R.id.price);


        }
    }
}