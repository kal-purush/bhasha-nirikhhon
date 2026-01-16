package com.example.classroommanagementsystemcms.Adapter;

import android.content.Context;
import android.view.LayoutInflater;
import android.view.View;
import android.view.ViewGroup;
import android.widget.TextView;

import androidx.annotation.NonNull;
import androidx.recyclerview.widget.RecyclerView;

import com.example.classroommanagementsystemcms.HelperClass.TeacherCourseList;
import com.example.classroommanagementsystemcms.R;

import java.util.ArrayList;

public class CourseListAdapter extends RecyclerView.Adapter<CourseListAdapter.ViewHolder> {

    private ArrayList<TeacherCourseList> dataModalArrayList;
    private Context context;

    public CourseListAdapter(ArrayList<TeacherCourseList> dataModalArrayList, Context context) {
        this.dataModalArrayList = dataModalArrayList;
        this.context = context;
    }


    @NonNull
    @Override
    public CourseListAdapter.ViewHolder onCreateViewHolder(@NonNull ViewGroup parent, int viewType) {
        return new CourseListAdapter.ViewHolder(LayoutInflater.from(context).inflate(R.layout.courses_horizontal_list, parent, false));
    }

    @Override
    public void onBindViewHolder(@NonNull CourseListAdapter.ViewHolder holder, int position) {

        final TeacherCourseList teacherCourseList = dataModalArrayList.get(position);
        holder.courseCodeTV.setText(teacherCourseList.getCourse_code());
        holder.courseNameTV.setText(teacherCourseList.getCourse_name());
        holder.YearTV.setText(teacherCourseList.getYear());
        holder.BatchTV.setText(teacherCourseList.getBatch());
        holder.coursecreditTV.setText(teacherCourseList.getCourse_credit());
        holder.obtainedclassTV.setText(teacherCourseList.getTotal_obtained_classes());
        holder.takenclassTV.setText(teacherCourseList.getTotal_taken_classes());


    }

    @Override
    public int getItemCount() {
        return dataModalArrayList.size();
    }

    public class ViewHolder extends RecyclerView.ViewHolder{

        private final TextView courseCodeTV;
        private final TextView courseNameTV;
        private final TextView YearTV;
        private final TextView BatchTV;
        private final TextView coursecreditTV;
        private final TextView obtainedclassTV;
        private final TextView takenclassTV;


        public ViewHolder(@NonNull View itemView) {
            super(itemView);

            courseCodeTV = itemView.findViewById(R.id.course_code);
            courseNameTV = itemView.findViewById(R.id.course_name);
            YearTV = itemView.findViewById(R.id.year);
            BatchTV = itemView.findViewById(R.id.batch);
            coursecreditTV = itemView.findViewById(R.id.course_credit);
            obtainedclassTV = itemView.findViewById(R.id.total_obtained_classes);
            takenclassTV = itemView.findViewById(R.id.total_taken_classes);

        }
    }
}