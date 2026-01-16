package com.example.prm_shop.Adapter;

import android.view.LayoutInflater;
import android.view.View;
import android.view.ViewGroup;
import android.widget.TextView;

import androidx.annotation.NonNull;
import androidx.recyclerview.widget.RecyclerView;

import com.example.prm_shop.R;
import com.example.prm_shop.models.response.CartItemResponse;

import java.util.List;

public class CartAdapter extends RecyclerView.Adapter<CartAdapter.CartViewHolder> {

    private List<CartItemResponse> items;

    public void setItems(List<CartItemResponse> items) {
        this.items = items;
        notifyDataSetChanged();
    }

    @NonNull
    @Override
    public CartViewHolder onCreateViewHolder(@NonNull ViewGroup parent, int viewType) {
        View view = LayoutInflater.from(parent.getContext()).inflate(R.layout.item_cart, parent, false);
        return new CartViewHolder(view);
    }

    @Override
    public void onBindViewHolder(@NonNull CartViewHolder holder, int position) {
        CartItemResponse item = items.get(position);
        holder.bind(item);
    }

    @Override
    public int getItemCount() {
        return items != null ? items.size() : 0;
    }

    static class CartViewHolder extends RecyclerView.ViewHolder {

        private TextView productNameTextView;
        private TextView quantityTextView;
        private TextView unitPriceTextView;

        public CartViewHolder(@NonNull View itemView) {
            super(itemView);
            productNameTextView = itemView.findViewById(R.id.text_product_name);
            quantityTextView = itemView.findViewById(R.id.text_quantity);
            unitPriceTextView = itemView.findViewById(R.id.text_unit_price);
        }

        public void bind(CartItemResponse item) {
            productNameTextView.setText(item.getProductName());
            quantityTextView.setText("Quantity: " + item.getQuantity());
            unitPriceTextView.setText("Unit Price: $" + item.getUnitPrice());
        }
    }
}