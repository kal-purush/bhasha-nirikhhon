package com.example.enigmator.activity;

import android.os.Bundle;
import android.support.v7.widget.LinearLayoutManager;
import android.support.v7.widget.RecyclerView;
import android.util.Log;
import android.view.MenuItem;
import android.view.View;
import android.widget.ProgressBar;
import android.widget.TextView;

import com.example.enigmator.R;
import com.example.enigmator.controller.HttpRequest;
import com.example.enigmator.controller.InviteRecyclerViewAdapter;
import com.example.enigmator.entity.Response;
import com.example.enigmator.entity.UserEnigmator;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;

import java.util.ArrayList;
import java.util.List;

import static com.example.enigmator.controller.HttpRequest.GET;
import static com.example.enigmator.controller.HttpRequest.POST;

public class FriendInviteActivity extends HttpActivity {
    private static final String TAG = FriendInviteActivity.class.getName();

    private ProgressBar progressLoading;
    private TextView textEmpty;

    private InviteRecyclerViewAdapter adapter;
    private List<UserEnigmator> friendRequesters;

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_friend_invite);

        progressLoading = findViewById(R.id.progress_loading);
        textEmpty = findViewById(R.id.text_empty);

        @SuppressWarnings("ConstantConditions")
        final int currentUserId = UserEnigmator.getCurrentUser(this).getId();

        OnListInteractionListener rejectListener = new OnListInteractionListener() {
            @Override
            public void onListInteractionListener(int userId) {
                httpManager.addToQueue(POST, "/UserEnigmators/" + userId + "/DenyAFriendRequest", null, null);
                removeForId(userId);
            }
        };

        OnListInteractionListener acceptListener = new OnListInteractionListener() {
            @Override
            public void onListInteractionListener(int userId) {
                httpManager.addToQueue(POST, "/UserEnigmators/" + userId + "/AcceptAFriendRequest", null, null);
                removeForId(userId);
            }
        };

        adapter = new InviteRecyclerViewAdapter(rejectListener, acceptListener);

        RecyclerView recyclerView = findViewById(R.id.list_invites);
        recyclerView.setLayoutManager(new LinearLayoutManager(FriendInviteActivity.this));
        recyclerView.setAdapter(adapter);

        httpManager.addToQueue(GET, "/friends?filter[where][state]=request&filter[where][ID_TO]="
                + currentUserId + "&filter[include]=FROM", null, new HttpRequest.HttpRequestListener() {
            @Override
            public void prepareRequest() {
                progressLoading.setVisibility(View.VISIBLE);
            }

            @Override
            public void handleSuccess(Response response) {
                progressLoading.setVisibility(View.GONE);

                if (response.getStatusCode() == 204) {
                    textEmpty.setVisibility(View.VISIBLE);
                } else {
                    JsonArray array = gson.fromJson(response.getContent(), JsonArray.class);

                    friendRequesters = new ArrayList<>();
                    for (JsonElement object : array) {
                        friendRequesters.add(gson.fromJson(object.getAsJsonObject().get("FROM"), UserEnigmator.class));
                    }

                    adapter.setValues(friendRequesters);
                    adapter.notifyDataSetChanged();
                }
            }

            @Override
            public void handleError(Response error) {
                progressLoading.setVisibility(View.GONE);
                textEmpty.setVisibility(View.VISIBLE);

                Log.e(TAG, "/friends?filter[where][state]=request&filter[where][ID_TO]="
                        + currentUserId + "&filter[include]=FROM");
                Log.e(TAG, error.toString());
            }
        });
    }

    @Override
    public boolean onOptionsItemSelected(MenuItem item) {
        if (item.getItemId() == android.R.id.home) {
            onBackPressed();
        }
        return true;
    }

    private void removeForId(int id) {
        for (UserEnigmator user : friendRequesters) {
            if (user.getId() == id) {
                friendRequesters.remove(user);
                break;
            }
        }
        adapter.notifyDataSetChanged();
        if (friendRequesters.isEmpty()) {
            textEmpty.setVisibility(View.VISIBLE);
        }
    }

    /**
     * This interface must be implemented by activities that contain this
     * fragment to allow an interaction in this fragment to be communicated
     * to the activity and potentially other fragments contained in that
     * activity.
     */
    public interface OnListInteractionListener {
        void onListInteractionListener(int userId);
    }
}