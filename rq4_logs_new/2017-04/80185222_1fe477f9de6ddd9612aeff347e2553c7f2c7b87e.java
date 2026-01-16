package com.example.lexlevi.sweapp.Adapters;

import android.content.Context;
import android.support.transition.TransitionManager;
import android.support.v7.widget.RecyclerView;
import android.view.LayoutInflater;
import android.view.View;
import android.view.ViewGroup;
import android.widget.ImageView;
import android.widget.TextView;

import com.example.lexlevi.sweapp.Models.Classmate;
import com.example.lexlevi.sweapp.Models.Course;
import com.example.lexlevi.sweapp.R;
import com.example.lexlevi.sweapp.Singletons.UserSession;

public class ClassmatesAdapter extends RecyclerView.Adapter<ClassmatesAdapter.ViewHolder> {

    private RecyclerView _recyclerView;

    private Context _context;
    private Classmate[] _classmates;
    private int _expandedPosition = -1;

    public ClassmatesAdapter(Context context, Classmate[] classmates, RecyclerView v) {
        super();
        _context = context;
        _classmates = classmates;
        _recyclerView = v;
    }

    @Override
    public ViewHolder onCreateViewHolder(ViewGroup parent, int viewType) {
        View v = LayoutInflater.from(parent.getContext())
                .inflate(R.layout.classmates_list_content, parent, false);
        ViewHolder viewHolder = new ViewHolder(v);
        return viewHolder;
    }

    @Override
    public void onBindViewHolder(ViewHolder holder, final int position) {
        Classmate classmate = _classmates[position];
        String matchingClasses = "";
        final boolean isExpanded = position == _expandedPosition;
        holder._textViewName.setText(classmate.getUser().getFirstName()
                + " " + classmate.getUser().getLastName() + " (" + classmate.getMatches() + ")");
        for (Course c : _classmates[position].getUser().getCourses()) {
            for (Course myC : UserSession.getInstance().getCurrentUser().getCourses()) {
                if (c.getCode().equals(myC.getCode())) {
                    matchingClasses += c.getName();
                    matchingClasses += "\r\n";
                }
            }
        }
        matchingClasses = matchingClasses.substring(0, matchingClasses.length() - 2);
        holder._textViewClasses.setText(matchingClasses);
        holder._textViewClasses.setVisibility(isExpanded ? View.VISIBLE : View.GONE);
        holder.itemView.setActivated(isExpanded);
        holder.itemView.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                _expandedPosition = isExpanded ? -1 : position;
                //TransitionManager.beginDelayedTransition(_recyclerView);
                notifyDataSetChanged();
            }
        });
    }

    @Override
    public int getItemCount() {
        return _classmates.length;
    }

    class ViewHolder extends RecyclerView.ViewHolder{
        public TextView _textViewName;
        public ImageView _imageViewLogo;
        public TextView _textViewClasses;

        public ViewHolder(View itemView) {
            super(itemView);
            _textViewName = (TextView) itemView.findViewById(R.id.classmate_name);
            _imageViewLogo = (ImageView) itemView.findViewById(R.id.classmate_logo);
            _textViewClasses = (TextView) itemView.findViewById(R.id.classmate_classes_in);
        }
    }

}