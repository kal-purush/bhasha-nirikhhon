package dev.frilly.locket.activities;

import android.os.Bundle;
import android.view.View;
import android.widget.Button;
import android.widget.ImageButton;

import androidx.annotation.Nullable;
import androidx.appcompat.app.AppCompatActivity;
import androidx.recyclerview.widget.LinearLayoutManager;
import androidx.recyclerview.widget.RecyclerView;

import java.util.ArrayList;
import java.util.List;

import dev.frilly.locket.R;
import dev.frilly.locket.adapter.FriendsListAdapter;
import dev.frilly.locket.room.entities.UserProfile;
import dev.frilly.locket.utils.AndroidUtil;

/**
 * The activity responsible for handling sending friend requests and displaying
 * friends, sent requests and accept requests
 */
public class FriendsActivity extends AppCompatActivity {

    private final List<UserProfile> fakeFriendsDataSource = new ArrayList<>();

    private ImageButton backButton;

    private RecyclerView friendsRecyclerView;
    private View friendsDecorativeView;
    private Button friendsMoreButton;

    @Override
    protected void onCreate(@Nullable Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.friends_screen);
        AndroidUtil.applyInsets(this, R.id.layout_outer);

        backButton = findViewById(R.id.button_back);

        friendsRecyclerView = findViewById(R.id.recycler_friends);
        friendsDecorativeView = findViewById(R.id.view_friends_decorative);
        friendsMoreButton = findViewById(R.id.button_friends_more);

        backButton.setOnClickListener(v -> getOnBackPressedDispatcher().onBackPressed());
        setupFriendsRecyclerView();
    }

    private void setupFriendsRecyclerView() {
        fakeFriendsDataSource.add(UserProfile.testUserProfile(1, "alice", "alice@email.com"));
        fakeFriendsDataSource.add(UserProfile.testUserProfile(2, "bob", "bob@email.com"));
        fakeFriendsDataSource.add(UserProfile.testUserProfile(3, "cindy", "cindy@email.com"));
        fakeFriendsDataSource.add(UserProfile.testUserProfile(4, "daisy", "daisy@email.com"));
        fakeFriendsDataSource.add(UserProfile.testUserProfile(5, "eli", "eli@email.com"));

        final var adapter = new FriendsListAdapter(this, true, (pos, a) -> {
            fakeFriendsDataSource.remove((int) pos);
            a.notifyItemRemoved(pos);
        });
        friendsRecyclerView.setAdapter(adapter);
        friendsRecyclerView.setLayoutManager(new LinearLayoutManager(this,
                RecyclerView.VERTICAL, false));
        adapter.setDataSource(fakeFriendsDataSource);
        friendsRecyclerView.forceLayout();
    }

}