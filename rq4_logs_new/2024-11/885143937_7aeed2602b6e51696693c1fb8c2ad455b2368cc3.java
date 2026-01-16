package com.example.gocourse.ui.adapters;

import android.view.LayoutInflater;
import android.view.View;
import android.view.ViewGroup;
import android.widget.TextView;
import androidx.annotation.NonNull;
import androidx.recyclerview.widget.RecyclerView;
import com.example.gocourse.R;
import java.util.Calendar;
import java.util.List;

public class CalendarAdapter extends RecyclerView.Adapter<CalendarAdapter.CalendarViewHolder> {
    private List<CalendarDay> days;
    private OnDayClickListener listener;

    public CalendarAdapter(List<CalendarDay> days) {
        this.days = days;
    }

    @NonNull
    @Override
    public CalendarViewHolder onCreateViewHolder(@NonNull ViewGroup parent, int viewType) {
        View view = LayoutInflater.from(parent.getContext())
                .inflate(R.layout.calendar_day_item, parent, false);
        return new CalendarViewHolder(view);
    }

    @Override
    public void onBindViewHolder(@NonNull CalendarViewHolder holder, int position) {
        CalendarDay day = days.get(position);
        holder.bind(day);
    }

    @Override
    public int getItemCount() {
        return days.size();
    }

    public void setOnDayClickListener(OnDayClickListener listener) {
        this.listener = listener;
    }

    class CalendarViewHolder extends RecyclerView.ViewHolder {
        TextView dateText;

        CalendarViewHolder(@NonNull View itemView) {
            super(itemView);
            dateText = itemView.findViewById(R.id.tv_date);
            
            itemView.setOnClickListener(v -> {
                int position = getAdapterPosition();
                if (position != RecyclerView.NO_POSITION && listener != null) {
                    listener.onDayClick(days.get(position));
                }
            });
        }

        void bind(CalendarDay day) {
            dateText.setText(String.valueOf(day.getDay()));
            dateText.setSelected(day.isSelected());
            dateText.setEnabled(day.isEnabled());
        }
    }

    public interface OnDayClickListener {
        void onDayClick(CalendarDay day);
    }

    public static class CalendarDay {
        private final Calendar calendar;
        private boolean isSelected;
        private boolean isEnabled;

        public CalendarDay(Calendar calendar, boolean isEnabled) {
            this.calendar = calendar;
            this.isEnabled = isEnabled;
        }

        public int getDay() {
            return calendar.get(Calendar.DAY_OF_MONTH);
        }

        public Calendar getCalendar() {
            return calendar;
        }

        public boolean isSelected() {
            return isSelected;
        }

        public void setSelected(boolean selected) {
            isSelected = selected;
        }

        public boolean isEnabled() {
            return isEnabled;
        }
    }
} 