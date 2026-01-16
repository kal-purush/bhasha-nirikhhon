package com.example.jalihara;

import android.view.LayoutInflater;
import android.view.Menu;
import android.view.View;
import android.view.ViewGroup;
import android.widget.Button;
import android.widget.ImageView;
import android.widget.TextView;

import androidx.annotation.NonNull;
import androidx.recyclerview.widget.RecyclerView;

import java.util.List;

public class  ItemHomeAdapter extends RecyclerView.Adapter<ItemHomeAdapter.ViewHolder> {
    private static ItemHomeAdapter.buttonClickListener buttonClickListener;
    private List<Item> itemList;

    public ItemHomeAdapter(List<Item> itemList, ItemHomeAdapter.buttonClickListener buttonClickListener) {
        this.itemList = itemList;
        this.buttonClickListener = buttonClickListener;
    }

    public List<Item> getItemList() {
        return itemList;
    }

    @NonNull
    @Override
    public ViewHolder onCreateViewHolder(@NonNull ViewGroup parent, int viewType) {
        View view = LayoutInflater.from(parent.getContext()).inflate(
                R.layout.activity_home_ticket, parent, false);

        // Passing view to ViewHolder
        return new ViewHolder(view, ItemHomeAdapter.buttonClickListener);
    }

    @Override
    public void onBindViewHolder(@NonNull ItemHomeAdapter.ViewHolder holder, int position) {
        // TypeCast Object to int type
        Item item = itemList.get(position);
        holder.bind(item);
    }

    @Override
    public int getItemCount() {
        return itemList.size();
    }

    public static class ViewHolder extends RecyclerView.ViewHolder implements View.OnClickListener {
        List<Item> itemList;
        ItemHomeAdapter.buttonClickListener buttonClickListener;
        TextView itemId;
        ImageView itemImage;
        TextView itemTitle;
        Button order_btn;


        public ViewHolder(View view, ItemHomeAdapter.buttonClickListener buttonClickListener) {
            super(view);
            itemId = view.findViewById(R.id.text_view_id);
            itemImage = view.findViewById(R.id.image_icon);
            itemTitle = view.findViewById(R.id.text_view_title);
            order_btn = view.findViewById(R.id.order_btn);

            this.buttonClickListener = buttonClickListener;
            order_btn.setOnClickListener(this);
            // view.setOnClickListener(this);
        }

        public void bind(Item item) {
            itemId.setText(item.getItemId());
            itemImage.setImageResource(item.getItemImage());
            itemTitle.setText(item.getItemTitle());
//            order_btn.setOnClickListener(new View.OnClickListener() {
//                @Override
//                public void onClick(View v) {
//                    buttonClickListener.onButtonClick(getAdapterPosition());
//                }
//            });
        }

        @Override
        public void onClick(View v) {
            buttonClickListener.onButtonClick(getAdapterPosition());
        }
    }

    public interface buttonClickListener{
        void onButtonClick(int position);

        boolean onCreateOptionsMenu(Menu menu);
    }
}