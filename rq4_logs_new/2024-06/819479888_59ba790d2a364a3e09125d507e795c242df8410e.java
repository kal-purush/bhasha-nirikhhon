package com.example.prm_shop.Adapter;

import android.content.Context;
import android.view.LayoutInflater;
import android.view.View;
import android.view.ViewGroup;
import android.widget.TextView;
import androidx.annotation.NonNull;
import androidx.recyclerview.widget.RecyclerView;

import com.example.prm_shop.R;
import com.example.prm_shop.models.response.OrderResponse;

import java.util.List;

public class OrderAdapter extends RecyclerView.Adapter<OrderAdapter.OrderViewHolder> {

    private Context context;
    private List<OrderResponse> orderList;

    public OrderAdapter(Context context, List<OrderResponse> orderList) {
        this.context = context;
        this.orderList = orderList;
    }

    public void setData(List<OrderResponse> orders) {
        this.orderList = orders;
        notifyDataSetChanged();
    }

    @NonNull
    @Override
    public OrderViewHolder onCreateViewHolder(@NonNull ViewGroup parent, int viewType) {
        View view = LayoutInflater.from(context).inflate(R.layout.order_item, parent, false);
        return new OrderViewHolder(view);
    }

    @Override
    public void onBindViewHolder(@NonNull OrderViewHolder holder, int position) {
        OrderResponse order = orderList.get(position);
        holder.bind(order);
    }

    @Override
    public int getItemCount() {
        return orderList.size();
    }

    public class OrderViewHolder extends RecyclerView.ViewHolder {

        private TextView orderCodeTextView, orderDateTextView, totalTextView;

        public OrderViewHolder(@NonNull View itemView) {
            super(itemView);
            orderCodeTextView = itemView.findViewById(R.id.orderCodeTextView);
            orderDateTextView = itemView.findViewById(R.id.orderDateTextView);
            totalTextView = itemView.findViewById(R.id.totalTextView);
        }

        public void bind(OrderResponse order) {
            orderCodeTextView.setText("Order Code: " + order.getOrderCode());
            orderDateTextView.setText("Order Date: " + order.getOrderDate());
            totalTextView.setText("Total: $" + order.getTotal());
        }
    }
}