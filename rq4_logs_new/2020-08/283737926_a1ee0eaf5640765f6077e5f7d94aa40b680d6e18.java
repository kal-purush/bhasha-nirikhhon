package com.example.geekbrainsandroidweather;

import android.view.LayoutInflater;
import android.view.View;
import android.view.ViewGroup;
import android.widget.TextView;

import androidx.annotation.NonNull;
import androidx.recyclerview.widget.RecyclerView;

public class RecyclerDataAdapter extends RecyclerView.Adapter<RecyclerDataAdapter.ViewHolder> {
    private String[] data;
    private final IRVOnItemClick onItemClickCallback;
    private final IRVOnItemClick changeItem;

    // конструктор. Адаптер используется в активити или фрагменте
    public RecyclerDataAdapter(String[] data, IRVOnItemClick onItemClickCallback, IRVOnItemClick changeItem) {
        this.data = data;
        this.onItemClickCallback = onItemClickCallback;
        this.changeItem = changeItem;
    }

    // Обращаемся к системному сервису LayoutInflater
    // соединяем каждый элемент массива с layout
    @NonNull
    @Override
    public ViewHolder onCreateViewHolder(@NonNull ViewGroup parent, int viewType) {
        View view = LayoutInflater.from(parent.getContext()).inflate(R.layout.rv_layout, parent,
                false);
        return new ViewHolder(view);
    }

    // Отображение даннных в view ViewHolder-а
    @Override
    public void onBindViewHolder(@NonNull ViewHolder holder, int position) {
        String text = data[position];
        holder.setTextToTextView(text);
        holder.setOnClickForItem(text, position);
        holder.changeView();
    }

    // возвращает количество элементов в массиве
    @Override
    public int getItemCount() {
        return data == null ? 0 : data.length;
    }

    // класс для работы с одной view
    class ViewHolder extends RecyclerView.ViewHolder {
        private TextView textView;

        public ViewHolder(@NonNull View itemView) {
            super(itemView);
            textView = itemView.findViewById(R.id.itemTextView);
        }

        void setTextToTextView(String text) {
            textView.setText(text);
        }

        void setOnClickForItem(final String text, final int position) {
            textView.setOnClickListener(new View.OnClickListener() {
                @Override
                public void onClick(View view) {
                    if(onItemClickCallback != null) {
                        onItemClickCallback.onItemClicked(text, position);
                    }
                }
            });
        }

        void changeView() {
            changeItem.changeItem(textView);
        }
    }
}