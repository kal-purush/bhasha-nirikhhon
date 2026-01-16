package hungnp12.demo.daubep;

import android.content.Intent;
import android.os.Bundle;
import android.view.View;
import android.widget.SearchView;

import androidx.appcompat.app.AppCompatActivity;
import androidx.recyclerview.widget.DividerItemDecoration;
import androidx.recyclerview.widget.LinearLayoutManager;
import androidx.recyclerview.widget.RecyclerView;

import java.util.ArrayList;
import java.util.List;

public class ChatActivity extends AppCompatActivity {

    private RecyclerView rcvUsers;
    private UserAdapter userAdapter;
    private SearchView searchView;

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_chat);

        rcvUsers = findViewById(R.id.rcvUsers);
        LinearLayoutManager linearLayoutManager = new LinearLayoutManager(this);
        rcvUsers.setLayoutManager(linearLayoutManager); // provides similar functionality to ListView

        userAdapter = new UserAdapter(this, getListUsers());
        rcvUsers.setAdapter(userAdapter);
        RecyclerView.ItemDecoration itemDecoration = new DividerItemDecoration(this, DividerItemDecoration.VERTICAL);
        rcvUsers.addItemDecoration(itemDecoration);


        searchView = findViewById(R.id.svInput);
        searchView.setOnQueryTextListener(new SearchView.OnQueryTextListener() {
            @Override
            public boolean onQueryTextSubmit(String query) {
                userAdapter.getFilter().filter(query);
                return false;
            }

            @Override
            public boolean onQueryTextChange(String newText) {
                userAdapter.getFilter().filter(newText);
                return false;
            }
        });
    }


    private List<User> getListUsers() {
        List<User> list = new ArrayList<>();
        list.add(new User(R.drawable.kontum, "Evil_Gwi", "10 phút trước", "Đã xem"));
        list.add(new User(R.drawable.becun, "Be Cun", "1 phút trước", "Đã nhận"));
        list.add(new User(R.drawable.toto, "To To", "20 phút trước", "Đã gửi"));
        list.add(new User(R.drawable.avatar_first, "Alex", "1 phút trước", "Đã nhận"));
        list.add(new User(R.drawable.avatar_second, "Nung Nguyen", "10 phút trước", "Đã gửi"));
        list.add(new User(R.drawable.avatar_third, "Teo Le", "20 phút trước", "Đã xem"));
        list.add(new User(R.drawable.avatar_fourth, "Tun Tun", "1 phút trước", "Đã xem"));
        list.add(new User(R.drawable.avatar_fifth, "Min Min", "10 phút trước", "Đã nhận"));
        list.add(new User(R.drawable.avatar_sixth, "Natto", "20 phút trước", "Đã gửi"));
        list.add(new User(R.drawable.avatar_seventh, "Ruby", "10 phút trước", "Đã gửi"));
        return list;
    }

    @Override
    protected void onDestroy() {
        super.onDestroy();
        if(userAdapter != null){
            userAdapter.release();
        }
    }
}