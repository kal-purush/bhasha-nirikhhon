package com.example.pulopo.Activity;

import androidx.appcompat.app.AppCompatActivity;
import androidx.appcompat.widget.Toolbar;
import androidx.recyclerview.widget.LinearLayoutManager;
import androidx.recyclerview.widget.RecyclerView;

import android.content.Intent;
import android.os.Bundle;
import android.view.View;
import android.widget.EditText;
import android.widget.ImageView;
import android.widget.Toast;

import com.example.pulopo.Adapter.UserAdapter;
import com.example.pulopo.R;
import com.example.pulopo.Retrofit.ApiServer;
import com.example.pulopo.Retrofit.RetrofitClient;
import com.example.pulopo.Utils.UserUtil;
import com.example.pulopo.Utils.UtilsCommon;
import com.example.pulopo.model.User;
import com.example.pulopo.model.response.ListChatUser;

import java.util.ArrayList;
import java.util.List;

import io.reactivex.rxjava3.android.schedulers.AndroidSchedulers;
import io.reactivex.rxjava3.disposables.CompositeDisposable;
import io.reactivex.rxjava3.schedulers.Schedulers;

public class SearchingActivity extends AppCompatActivity {
    Toolbar toolbar;
    RecyclerView recyclerView;
    UserAdapter userAdapter;
    ApiServer apiServer;
    CompositeDisposable compositeDisposable = new CompositeDisposable();
    String userName;
    ImageView search;

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_searching);
        initView();
        apiServer = RetrofitClient.getInstance(UtilsCommon.BASE_URL).create(ApiServer.class);
    }

    public void onBackPressed() {
        super.onBackPressed(); // Gọi phương thức gốc để hoạt động mặc định của nút "Back"
        // Các xử lý khác nếu cần
        Intent intent = new Intent(SearchingActivity.this,ConnectFriendsActivity.class);
        startActivity(intent);
    }


    private void initView() {
        toolbar = findViewById(R.id.toolbar_search);
        recyclerView = findViewById(R.id.recyclerview_search);
        RecyclerView.LayoutManager layoutManager = new LinearLayoutManager(this);
        recyclerView.setLayoutManager(layoutManager);
        recyclerView.setHasFixedSize(true);
        search = findViewById(R.id.image_search);
        search.setOnClickListener(view -> {
            userName = String.valueOf(((EditText) findViewById(R.id.input_search)).getText());
            if (userName == null || userName.equals("")) {
                Toast.makeText(getApplicationContext(), "Vui lòng nhập tên vào thanh tìm kiếm", Toast.LENGTH_SHORT).show();
            } else {
                compositeDisposable.add(apiServer.searchUsers(userName)
                        .subscribeOn(Schedulers.io())
                        .observeOn(AndroidSchedulers.mainThread())
                        .subscribe(
                                userModel -> {

                                    List<User> userList = new ArrayList<>();
                                    List<com.example.pulopo.model.response.User> listUser = userModel.getData();
                                    if (userModel.isSuccess()) {
                                        for (int i = 0; i < listUser.size(); i++) {
                                            User user = new User();
                                            user.setId((int) listUser.get(i).getid());
                                            user.setUsername(listUser.get(i).getUserName());
                                            userList.add(user);
                                        }
                                        if (userList.size() > 0) {
                                            userAdapter = new UserAdapter(getApplicationContext(), userList);
                                            recyclerView.setAdapter(userAdapter);
                                        }
                                    }
                                },
                                throwable -> {
                                    Toast.makeText(getApplicationContext(), throwable.getMessage(), Toast.LENGTH_LONG).show();
                                }
                        ));
            }
        });
    }
}