package com.example.nermingraca.bitbayapp;

import android.content.Intent;
import android.support.v7.app.ActionBarActivity;
import android.os.Bundle;
import android.view.Menu;
import android.view.MenuItem;
import android.view.View;
import android.widget.AdapterView;
import android.widget.ListView;
import android.widget.TextView;

import com.example.nermingraca.bitbayapp.models.Product;
import com.example.nermingraca.bitbayapp.models.User;
import com.example.nermingraca.bitbayapp.singletons.ProductFeed;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;


public class SellerActivity extends ActionBarActivity {

    private ListView mSellersProductList;

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_seller);


        Intent intent = getIntent();
        String username = intent.getStringExtra("username");
        String email = intent.getStringExtra("email");

        TextView usernameV = (TextView) findViewById(R.id.tvUsername);
        TextView emailV = (TextView) findViewById(R.id.tvEmail);

        usernameV.setText(username);
        emailV.setText(email);

        ProductFeed productFeed = ProductFeed.getInstance();
        productFeed.getFeed(getString(R.string.service_products));

        List<Product> products = productFeed.getFeed();

        List<Product> userProducts = new ArrayList<Product>();
        Iterator<Product> iterator = products.iterator();
        while (iterator.hasNext()) {
            Product temp = iterator.next();
            if (temp.getmOwner().equals(username)) {
                userProducts.add(temp);
            }
        }

        mSellersProductList = (ListView)findViewById(R.id.sellersList);

        CustomListAdapter productsAdapter = new CustomListAdapter
                (this, userProducts);

        mSellersProductList.setAdapter(productsAdapter);

        mSellersProductList.setOnItemClickListener(new AdapterView.OnItemClickListener() {
            @Override
            public void onItemClick(AdapterView<?> parent, View view, int position, long id) {
                Product clicked = (Product) parent.getItemAtPosition(position);
                Intent intent = new Intent(SellerActivity.this, ProductActivity.class);
                intent.putExtra("id", clicked.getmId());
                intent.putExtra("name", clicked.getmName());
                intent.putExtra("description", clicked.getmDescription());
                intent.putExtra("imagePath", clicked.getThumbnailUrl());
                intent.putExtra("seller", clicked.getmOwner());
                double priceDouble = clicked.getmPrice();
                String price = String.format("$" + "%.2f", priceDouble);
                intent.putExtra("price", price);
                startActivity(intent);
            }
        });
    }


    @Override
    public boolean onCreateOptionsMenu(Menu menu) {
        // Inflate the menu; this adds items to the action bar if it is present.
        getMenuInflater().inflate(R.menu.menu_seller, menu);
        return true;
    }

    @Override
    public boolean onOptionsItemSelected(MenuItem item) {
        // Handle action bar item clicks here. The action bar will
        // automatically handle clicks on the Home/Up button, so long
        // as you specify a parent activity in AndroidManifest.xml.
        int id = item.getItemId();

        //noinspection SimplifiableIfStatement
        if (id == R.id.action_settings) {
            return true;
        }

        if (id == R.id.logout_action) {
            MainActivity.logout();
            moveTaskToBack(true);
            Intent toLogin = new Intent( SellerActivity.this, MainActivity.class);
            startActivity(toLogin);
            return true;
        }

        if (id == R.id.profile_action) {
            User user = ProfileActivity.getCurrentUser();
            moveTaskToBack(true);
            Intent intent = new Intent(SellerActivity.this, ProfileActivity.class);
            intent.putExtra("username", user.getmUsername());
            intent.putExtra("email", user.getmEmail());
            startActivity(intent);
            return true;
        }

        return super.onOptionsItemSelected(item);
    }
}