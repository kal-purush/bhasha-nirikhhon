package com.example.prm_shop.activities;

import android.annotation.SuppressLint;
import android.content.Intent;
import android.os.Bundle;
import android.view.View;
import android.widget.Button;

import androidx.appcompat.app.AppCompatActivity;

import com.example.prm_shop.R;

public class ProductOptionActivity extends ManagePageActivity {
    private Button btnProductList, btnCreateProduct;


    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_product_option); // Use the correct layout file

        btnProductList = findViewById(R.id.btn_product_list);
        btnCreateProduct = findViewById(R.id.btn_create_product);

        btnProductList.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                Intent intent = new Intent(ProductOptionActivity.this, ProductManageActivity.class);
                startActivity(intent);
            }
        });

        btnCreateProduct.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                Intent intent = new Intent(ProductOptionActivity.this, CreateProductActivity.class);
                startActivity(intent);
            }
        });
    }
}