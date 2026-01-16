package com.example.myapplication;

import android.content.Context;
import android.graphics.Color;
import android.graphics.PorterDuff;
import android.view.LayoutInflater;
import android.view.View;
import android.view.ViewGroup;
import android.widget.ImageView;
import android.widget.TextView;

import androidx.recyclerview.widget.RecyclerView;

import java.util.List;

public class ListAdd extends RecyclerView.Adapter<ListAdd.ViewHolder> {

    private List<List_element> list_data;
    private LayoutInflater inflater;
    private Context context;



    public ListAdd(List<List_element> items_list, Context context){

        this.inflater = LayoutInflater.from(context);
        this.context = context;
        this.list_data = items_list;

    }

    @Override
    public int getItemCount() { return list_data.size(); }


    public  ListAdd.ViewHolder onCreateViewHolder(ViewGroup parent, int viewType){
        View view = inflater.inflate(R.layout.list_element, null);
        return new ListAdd.ViewHolder(view);
    }

    @Override
    public void onBindViewHolder(final ListAdd.ViewHolder holder, final int position){
        holder.bindData(list_data.get(position));
    }

    public void setItems(List<List_element> items) {
        list_data = items;
    }

    public class ViewHolder extends RecyclerView.ViewHolder{

        ImageView iconImage;
        TextView name, description, status;

        ViewHolder(View itemView){
            super(itemView);
            name = itemView.findViewById(R.id.name_task);
            description = itemView.findViewById(R.id.task_description);
            status = itemView.findViewById(R.id.task_status);

        }

        void bindData (final List_element item){
           // iconImage.setColorFilter(Color.parseColor(item.getColor()), PorterDuff.Mode.SRC_IN);
            name.setText(item.getName());
            description.setText(item.getDescription());
            status.setText(item.getStatus());
        }


    }


}