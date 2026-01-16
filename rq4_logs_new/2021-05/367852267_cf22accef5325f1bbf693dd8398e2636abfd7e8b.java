package com.example.taskmaster;

import android.view.View;
import android.view.ViewGroup;
import android.widget.TextView;

import androidx.annotation.NonNull;
import androidx.recyclerview.widget.RecyclerView;

import org.w3c.dom.Text;

public class TaskAdapter extends RecyclerView.Adapter<TaskAdapter.ViewHolder> {
//    private String[] localDataSet;

    /**
     * Provide a reference to the type of views that you are using
     * (custom ViewHolder).
     */
    public static class ViewHolder extends RecyclerView.ViewHolder {
        private final TextView taskTitle;
        private final TextView taskBody;
        private final TextView taskState;

        public TextView getTaskTitle() {
            return taskTitle;
        }

        public TextView getTaskBody() {
            return taskBody;
        }

        public TextView getTaskState() {
            return taskState;
        }

        public ViewHolder(View view) {
            super(view);
            // Define click listener for the ViewHolder's View

            taskTitle = (TextView) view.findViewById(R.id.textView11);
            taskBody = (TextView) view.findViewById(R.id.textView12);
            taskState = (TextView) view.findViewById(R.id.textView13);

        }

        @Override
        public void onClick(final View view) {
//            int itemPosition = mRecyclerView.getChildLayoutPosition(view);
//            String item = mList.get(itemPosition);
//            Toast.makeText(mContext, item, Toast.LENGTH_LONG).show();
        }

        @NonNull
        @Override
        public ViewHolder onCreateViewHolder(@NonNull ViewGroup parent, int viewType) {
//            View view = LayoutInflater.from(mContext).inflate(R.layout.myview, parent, false);
//            view.setOnClickListener(mOnClickListener);
//            return new MyViewHolder(view);
            return null;
        }

        @Override
        public void onBindViewHolder(@NonNull ViewHolder holder, int position) {

        }

        @Override
        public int getItemCount() {
            return 0;
        }

        public class ViewHolder {
        }
    }
}