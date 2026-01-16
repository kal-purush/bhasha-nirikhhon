package com.example.projectmilkshop.Activity;

import android.content.Intent;
import android.os.Bundle;
import android.util.Log;
import android.view.View;
import android.widget.LinearLayout;

import androidx.activity.EdgeToEdge;
import androidx.appcompat.app.AppCompatActivity;
import androidx.recyclerview.widget.LinearLayoutManager;
import androidx.recyclerview.widget.RecyclerView;

import com.example.projectmilkshop.Adapter.OrderAdapter;
import com.example.projectmilkshop.Api.OrderRepository;
import com.example.projectmilkshop.Api.OrderService;
import com.example.projectmilkshop.Domain.Order;
import com.example.projectmilkshop.Interceptor.SessionManager;
import com.example.projectmilkshop.R;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import retrofit2.Call;
import retrofit2.Callback;
import retrofit2.Response;

public class MyOrderActivity extends AppCompatActivity {

    private RecyclerView recyclerView;
    private OrderAdapter orderAdapter;
    private OrderService orderService;
    private SessionManager sessionManager;
    private String jwtToken;
    private List<Order> orderList;

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        EdgeToEdge.enable(this);
        setContentView(R.layout.activity_my_order);

        recyclerView = findViewById(R.id.recyclerView);

        sessionManager = new SessionManager(getApplicationContext());
        jwtToken = sessionManager.getJwtToken();
        orderService = OrderRepository.getOrderService(jwtToken);
        orderList = new ArrayList<>();
        fetchOrders();
        bottomNavigation();
    }

    private void fetchOrders() {
        Call<Order[]> call = orderService.GetOrdersOfAccount();
        call.enqueue(new Callback<Order[]>() {
            @Override
            public void onResponse(Call<Order[]> call, Response<Order[]> response) {
                if(response.isSuccessful() && response.body() != null) {
                    Order[] orders = response.body();
                    orderList.addAll(Arrays.asList(orders));
                    orderAdapter = new OrderAdapter(orderList);
                    recyclerView.setLayoutManager(new LinearLayoutManager(getApplicationContext()));
                    recyclerView.setAdapter(orderAdapter);
                } else {
                    Log.e("MyOrdersActivity", "Error: " + response.code());
                }
            }

            @Override
            public void onFailure(Call<Order[]> call, Throwable t) {
                Log.e("MyOrdersActivity", "Failure: " + t.getMessage());
            }
        });
    }

    private void bottomNavigation() {
        LinearLayout homebtn = findViewById(R.id.homeBtn);
        LinearLayout profilebtn = findViewById(R.id.profilebtn);
        LinearLayout cartbtn = findViewById(R.id.cartbtn);
        LinearLayout supportbtn = findViewById(R.id.supportbtn);
        LinearLayout mapbtn = findViewById(R.id.btnMap);

        homebtn.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                startActivity(new Intent(MyOrderActivity.this, MainActivity.class));
            }
        });
        profilebtn.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                startActivity(new Intent(MyOrderActivity.this, ProductActivity.class));
            }
        });
        cartbtn.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                startActivity(new Intent(MyOrderActivity.this, CartActivity.class));
            }
        });
        supportbtn.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                startActivity(new Intent(MyOrderActivity.this, ChatboxActivity.class));
            }
        });
        mapbtn.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                startActivity(new Intent(MyOrderActivity.this, MapActivity.class));
            }
        });
    }
}