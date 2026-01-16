package kr.ac.kpu.block.smared;

import android.os.Bundle;
import android.support.v7.app.AppCompatActivity;
import android.support.v7.widget.LinearLayoutManager;
import android.widget.Toast;

import com.google.firebase.auth.FirebaseAuth;
import com.google.firebase.database.DataSnapshot;
import com.google.firebase.database.DatabaseError;
import com.google.firebase.database.DatabaseReference;
import com.google.firebase.database.FirebaseDatabase;
import com.google.firebase.database.ValueEventListener;

import java.util.ArrayList;
import java.util.List;

import kr.ac.kpu.block.smared.databinding.ActivityFriendBinding;

public class FriendActivity extends AppCompatActivity {
    private FormattedLogger logger = new FormattedLogger();
    private ActivityFriendBinding viewBinding;

    List<UserInfo> friends = new ArrayList<>();

    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        viewBinding = ActivityFriendBinding.inflate(getLayoutInflater());
        setContentView(viewBinding.getRoot());

        readFriendDB();

        viewBinding.rvFriend.setHasFixedSize(true);
        viewBinding.rvFriend.setLayoutManager(new LinearLayoutManager(this));
        viewBinding.rvFriend.setAdapter(new FriendAdapter(friends,this));
    }

    // [Refactor] 사용자 정보를 미리 읽어서 전역변수에 두면 필요없어지는 함수
    // 1. 사용자 정보에서 친구 목록 Uid를 읽어온다.
    private void readFriendDB() {
        String userUid = FirebaseAuth.getInstance().getCurrentUser().getUid();
        String databasePath = "users" + "/"+ userUid + "/" + "friends";
        DatabaseReference userDBRef = FirebaseDatabase.getInstance().getReference(databasePath);
        userDBRef.addListenerForSingleValueEvent(new ValueEventListener() {
            @Override
            public void onDataChange(DataSnapshot userSnapshot) {
                // 2. 친구 목록의 사용자 정보를 user에서 가져온다.
                for (DataSnapshot friendSnapshot : userSnapshot.getChildren()) {
                    readUserDB(friendSnapshot.getValue().toString());
                }
            }

            @Override
            public void onCancelled(DatabaseError databaseError) {
                Toast.makeText(FriendActivity.this, "Failed to read value : " + databaseError.toException().getMessage(), Toast.LENGTH_SHORT).show();
                logger.writeLog("Failed to read value : " + databaseError.toException().getMessage());
            }
        });
    }

    // 2. 친구 목록의 사용자 정보를 user에서 가져온다.
    private void readUserDB(String userUid) {
        String databasePath = "users" + "/"+ userUid;
        DatabaseReference friendDBRef = FirebaseDatabase.getInstance().getReference(databasePath);
        friendDBRef.addListenerForSingleValueEvent(new ValueEventListener() {
            @Override
            public void onDataChange(DataSnapshot dataSnapshot) {
                friends.add(dataSnapshot.getValue(UserInfo.class));
            }

            @Override
            public void onCancelled(DatabaseError databaseError) {
                Toast.makeText(FriendActivity.this, "Failed to read value : " + databaseError.toException().getMessage(), Toast.LENGTH_SHORT).show();
                logger.writeLog("Failed to read value : " + databaseError.toException().getMessage());
            }
        });
    }
}