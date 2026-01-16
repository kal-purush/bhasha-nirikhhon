package com.example.geomessages.ui;

import android.view.LayoutInflater;
import android.view.View;
import android.view.ViewGroup;
import android.widget.ImageView;
import android.widget.TextView;

import androidx.annotation.NonNull;
import androidx.recyclerview.widget.RecyclerView;

import com.example.geomessages.R;
import com.example.geomessages.model.Message;
import com.squareup.picasso.Callback;
import com.squareup.picasso.Picasso;

import java.util.ArrayList;

public class MessageAdapter extends RecyclerView.Adapter<MessageAdapter.ViewHolder> {
    private ArrayList<Message> messages;

    public MessageAdapter(ArrayList<Message> messages) {
        this.messages = messages;
    }

    @NonNull
    @Override
    public ViewHolder onCreateViewHolder(@NonNull ViewGroup parent, int viewType) {
        View view = LayoutInflater.from(parent.getContext()).inflate(R.layout.message_row, parent, false);
        return new MessageAdapter.ViewHolder(view);
    }

    @Override
    public void onBindViewHolder(@NonNull ViewHolder holder, int position) {
        Message message = messages.get(position);
        holder.tvFirstName.setText(message.getFirstname());
        holder.tvLastName.setText(message.getLastname());
        holder.tvMessage.setText(message.getMessage());
        holder.tvLatitude.setText(message.getLatitude());
        holder.tvLongitude.setText(message.getLongitude());

        String url = message.getPicture();
        Picasso.get().load(url)
                .fetch(new Callback() {
                    @Override
                    public void onSuccess() {
                        Picasso.get().load(url)
                                .placeholder(R.drawable.ic_baseline_account_circle_24)
                                .into(holder.ivPicture);
                    }

                    @Override
                    public void onError(Exception e) {
                        Picasso.get().load(R.drawable.ic_baseline_account_circle_24)
                                .into(holder.ivPicture);
                    }
                });


    }

    @Override
    public int getItemCount() {
        return messages.size();
    }

    public class ViewHolder extends RecyclerView.ViewHolder {
        private final TextView tvFirstName;
        private final TextView tvLastName;
        private final TextView tvMessage;
        private final TextView tvLatitude;
        private final TextView tvLongitude;
        private final ImageView ivPicture;

        public ViewHolder(@NonNull View itemView) {
            super(itemView);
            tvFirstName = itemView.findViewById(R.id.tv_firstname);
            tvLastName = itemView.findViewById(R.id.tv_lastname);
            tvMessage = itemView.findViewById(R.id.tv_message);
            tvLatitude = itemView.findViewById(R.id.tv_latitude);
            tvLongitude = itemView.findViewById(R.id.tv_longitude);
            ivPicture = itemView.findViewById(R.id.iv_picture);
        }
    }
}