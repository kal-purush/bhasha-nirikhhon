package br.edu.ifce.engcomp.francis.diversidade.adapters;

import android.content.Context;
import android.support.v7.widget.RecyclerView;
import android.view.LayoutInflater;
import android.view.View;
import android.view.ViewGroup;
import android.widget.ImageView;
import android.widget.TextView;

import java.util.ArrayList;

import br.edu.ifce.engcomp.francis.diversidade.R;
import br.edu.ifce.engcomp.francis.diversidade.interfaces.RecyclerViewOnClickListenerHack;
import br.edu.ifce.engcomp.francis.diversidade.model.Developer;

/**
 * Created by Joamila on 30/03/2016.
 */
public class AboutRecyclerViewAdapter extends RecyclerView.Adapter<AboutRecyclerViewAdapter.ViewHolder> {
    private ArrayList<Developer> dataSource;
    private Context context;
    private RecyclerViewOnClickListenerHack mRecycleViewOnClickListenerHack;

    public AboutRecyclerViewAdapter(ArrayList<Developer> dataSource, Context context) {
        this.dataSource = dataSource;
        this.context = context;
    }

    public Developer getItem(int position) {
        return this.dataSource.get(position);
    }

    public void setRecycleViewOnClickListenerHack(RecyclerViewOnClickListenerHack rView) {
        this.mRecycleViewOnClickListenerHack = rView;
    }

    @Override
    public ViewHolder onCreateViewHolder(ViewGroup parent, int viewType) {
        LayoutInflater inflater = LayoutInflater.from(parent.getContext());
        View itemView = inflater.inflate(R.layout.adapter_developers, parent, false);

        return new AboutRecyclerViewAdapter.ViewHolder(itemView);

    }

    @Override
    public void onBindViewHolder(ViewHolder holder, int position) {
        Developer developer = this.dataSource.get(position);

        holder.nameTextView.setText(developer.getName());
    }

    @Override
    public int getItemCount() {
        return this.dataSource.size();
    }

    public class ViewHolder extends RecyclerView.ViewHolder implements View.OnClickListener{
        private TextView nameTextView;
        private ImageView facebookImageView;
        private ImageView twitterImageView;
        private ImageView instagramImageView;
        private ImageView githubImageView;

        public ViewHolder(View itemView) {
            super(itemView);

            this.nameTextView  = (TextView)  itemView.findViewById(R.id.name_dev_text_view);
            this.facebookImageView = (ImageView) itemView.findViewById(R.id.ic_facebook);
            this.twitterImageView = (ImageView) itemView.findViewById(R.id.ic_twitter);
            this.instagramImageView = (ImageView) itemView.findViewById(R.id.ic_instagram);
            this.githubImageView = (ImageView) itemView.findViewById(R.id.ic_github);

            itemView.setOnClickListener(this);
        }

        @Override
        public void onClick(View view) {
            if(mRecycleViewOnClickListenerHack != null){
                mRecycleViewOnClickListenerHack.onClickListener(view, getAdapterPosition());
            }
        }
    }
}