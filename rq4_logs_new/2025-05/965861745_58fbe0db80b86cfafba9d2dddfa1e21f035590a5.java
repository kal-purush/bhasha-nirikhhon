package com.daclink.citypulse;

import android.os.Handler;
import android.os.Looper;
import android.view.LayoutInflater;
import android.view.View;
import android.view.ViewGroup;
import android.widget.Button;
import android.widget.TextView;
import android.widget.Toast;

import androidx.annotation.NonNull;
import androidx.appcompat.app.AppCompatActivity;
import androidx.recyclerview.widget.RecyclerView;

import java.util.ArrayList;
import java.util.List;

public class UserAdapter extends RecyclerView.Adapter<UserAdapter.UserViewHolder> {
    // Full list of users
    private List<User> fullUserList;
    // Currently displayed list
    private List<User> filteredUserList;
    private final UserDao userDao;

    public UserAdapter(List<User> users, UserDao dao) {
        this.fullUserList = new ArrayList<>(users);
        this.filteredUserList = new ArrayList<>(users);
        this.userDao = dao;
    }

    public void filter(String query) {
        filteredUserList.clear();
        if (query.isEmpty()) {
            filteredUserList.addAll(fullUserList);
        } else {
            for (User user : fullUserList) {
                if (user.getUsername().toLowerCase().contains(query.toLowerCase())) {
                    filteredUserList.add(user);
                }
            }
        }
        notifyDataSetChanged();
    }

    @NonNull
    @Override
    public UserViewHolder onCreateViewHolder(@NonNull ViewGroup parent, int viewType) {
        View itemView = LayoutInflater.from(parent.getContext())
                .inflate(R.layout.user_row, parent, false);
        return new UserViewHolder(itemView);
    }

    @Override
    public void onBindViewHolder(@NonNull UserViewHolder holder, int position) {
        User user = filteredUserList.get(position);
        holder.usernameTextView.setText(user.getUsername());

        holder.deleteButton.setOnClickListener(v -> {
            AppDatabase.databaseWriteExecutor.execute(() -> {
                userDao.delete(user);
                List<User> updatedList = userDao.getAllUsers();  // Get fresh data

                Handler handler = new Handler(Looper.getMainLooper());
                handler.post(() -> {
                    updateList(updatedList);
                    Toast.makeText(v.getContext(), "User deleted", Toast.LENGTH_SHORT).show();
                });
            });
        });
    }

    @Override
    public int getItemCount() {
        return filteredUserList.size();
    }

    static class UserViewHolder extends RecyclerView.ViewHolder {
        TextView usernameTextView;
        Button deleteButton;

        public UserViewHolder(@NonNull View itemView) {
            super(itemView);
            usernameTextView = itemView.findViewById(R.id.usernameText);
            deleteButton = itemView.findViewById(R.id.deleteButton);
        }
    }
    public void updateList(List<User> newList) {
        fullUserList.clear();
        fullUserList.addAll(newList);
        filter(""); // reapply filter
    }


}