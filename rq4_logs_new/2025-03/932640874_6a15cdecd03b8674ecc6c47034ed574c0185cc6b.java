package dev.frilly.locket.activities;

import android.os.Bundle;
import android.util.Log;

import androidx.fragment.app.Fragment;
import androidx.recyclerview.widget.LinearLayoutManager;
import androidx.recyclerview.widget.RecyclerView;

import android.view.LayoutInflater;
import android.view.View;
import android.view.ViewGroup;

import com.firebase.ui.firestore.FirestoreRecyclerOptions;
import com.google.firebase.firestore.Query;

import dev.frilly.locket.R;
import dev.frilly.locket.model.Chatroom;
import dev.frilly.locket.utils.FirebaseUtil;
import dev.frilly.locket.adapter.RecentChatRecyclerAdapter;

public class ChatFragment extends Fragment {

    RecyclerView recyclerView;
    RecentChatRecyclerAdapter adapter;

    public ChatFragment() {}

    @Override
    public View onCreateView(LayoutInflater inflater, ViewGroup container,
                             Bundle savedInstanceState) {
        View view = inflater.inflate(R.layout.fragment_chat, container, false);
        recyclerView = view.findViewById(R.id.recyler_view);

        // Gọi Firebase để lấy userId trước khi setup RecyclerView
        FirebaseUtil.getCurrentUserId(userId -> {
            if (userId != null) {
                setupRecyclerView(userId);
            } else {
                Log.e("ChatFragment", "User ID is null. Cannot fetch chatrooms.");
            }
        });

        return view;
    }

    void setupRecyclerView(String userId) {
        Query query = FirebaseUtil.allChatroomCollectionReference()
                .whereArrayContains("userIds", userId)
                .orderBy("lastMessageTimestamp", Query.Direction.DESCENDING);

        FirestoreRecyclerOptions<Chatroom> options = new FirestoreRecyclerOptions.Builder<Chatroom>()
                .setQuery(query, Chatroom.class).build();

        adapter = new RecentChatRecyclerAdapter(options, requireContext());
        recyclerView.setLayoutManager(new LinearLayoutManager(requireContext()));
        recyclerView.setAdapter(adapter);
        adapter.startListening();
    }

    @Override
    public void onStart() {
        super.onStart();
        if (adapter != null) adapter.startListening();
    }

    @Override
    public void onStop() {
        super.onStop();
        if (adapter != null) adapter.stopListening();
    }

    @Override
    public void onResume() {
        super.onResume();
        if (adapter != null) adapter.notifyDataSetChanged();
    }
}