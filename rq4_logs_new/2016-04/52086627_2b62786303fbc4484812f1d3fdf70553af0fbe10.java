package br.edu.ifce.engcomp.francis.diversidade.adapters;

import android.content.Context;
import android.support.v7.widget.RecyclerView;
import android.view.LayoutInflater;
import android.view.View;
import android.view.ViewGroup;
import android.widget.TextView;

import java.util.ArrayList;

import br.edu.ifce.engcomp.francis.diversidade.R;
import br.edu.ifce.engcomp.francis.diversidade.interfaces.RecyclerViewOnClickListenerHack;
import br.edu.ifce.engcomp.francis.diversidade.model.Service;

/**
 * Created by Joamila on 10/04/2016.
 */
public class ServicesRecyclerViewAdapter extends RecyclerView.Adapter<ServicesRecyclerViewAdapter.ViewHolder>{
    private ArrayList<Service> dataSource;
    private Context context;
    private RecyclerViewOnClickListenerHack mRecycleViewOnClickListenerHack;

    public ServicesRecyclerViewAdapter(Context context, ArrayList<Service> services) {
        this.dataSource = services;
        this.context = context;
    }


    @Override
    public ServicesRecyclerViewAdapter.ViewHolder onCreateViewHolder(ViewGroup parent, int viewType) {
        LayoutInflater inflater = LayoutInflater.from(parent.getContext());
        View itemView = inflater.inflate(R.layout.adapter_services_nucleus,parent,false);
        return new ViewHolder (itemView);
    }

    @Override
    public void onBindViewHolder(ServicesRecyclerViewAdapter.ViewHolder holder, int position) {
        Service service = this.dataSource.get(position);

        holder.nameTextView.setText(service.getName());
        holder.categoryTextView.setText(service.getCategory());
    }

    @Override
    public int getItemCount() {
        return dataSource.size();
    }

    public class ViewHolder extends RecyclerView.ViewHolder implements View.OnClickListener{
        private TextView nameTextView;
        private TextView  categoryTextView;

        public ViewHolder(View itemView) {
            super(itemView);

            this.nameTextView = (TextView) itemView.findViewById(R.id.adapter_name_service);
            this.categoryTextView = (TextView) itemView.findViewById(R.id.adapter_category_service);

        }

        @Override
        public void onClick(View view) {
            if(mRecycleViewOnClickListenerHack != null){
                mRecycleViewOnClickListenerHack.onClickListener(view, getAdapterPosition());
            }
        }
    }
}