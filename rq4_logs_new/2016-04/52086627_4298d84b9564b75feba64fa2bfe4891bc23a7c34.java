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
import br.edu.ifce.engcomp.francis.diversidade.model.Comment;

/**
 * Created by Joamila on 10/04/2016.
 */
public class CommentsRecyclerViewAdapter extends RecyclerView.Adapter<CommentsRecyclerViewAdapter.ViewHolder>{
    private ArrayList<Comment> dataSource;
    private Context context;
    private RecyclerViewOnClickListenerHack mRecycleViewOnClickListenerHack;

    public CommentsRecyclerViewAdapter(Context context, ArrayList<Comment> comments) {
        this.dataSource = comments;
        this.context = context;
    }

    @Override
    public CommentsRecyclerViewAdapter.ViewHolder onCreateViewHolder(ViewGroup parent, int viewType) {
        LayoutInflater inflater = LayoutInflater.from(parent.getContext());
        View itemView = inflater.inflate(R.layout.adapter_comments_nucleus,parent,false);
        return new ViewHolder (itemView);
    }

    @Override
    public void onBindViewHolder(CommentsRecyclerViewAdapter.ViewHolder holder, int position) {
        Comment comment = this.dataSource.get(position);

        holder.authorTextView.setText(comment.getAuthor());
        holder.dateTextView.setText(comment.getDate().toString());
        holder.contentTextView.setText(comment.getContent());
    }

    @Override
    public int getItemCount() {
        return dataSource.size();
    }

    public class ViewHolder extends RecyclerView.ViewHolder implements View.OnClickListener{
        private TextView authorTextView;
        private TextView  dateTextView;
        private TextView contentTextView;

        public ViewHolder(View itemView) {
            super(itemView);

            this.authorTextView = (TextView) itemView.findViewById(R.id.adapter_comment_author_name);
            this.dateTextView = (TextView) itemView.findViewById(R.id.adapter_comment_timestamp);
            this.contentTextView = (TextView) itemView.findViewById(R.id.adapter_comment_content);

        }

        @Override
        public void onClick(View view) {
            if(mRecycleViewOnClickListenerHack != null){
                mRecycleViewOnClickListenerHack.onClickListener(view, getAdapterPosition());
            }
        }
    }
}