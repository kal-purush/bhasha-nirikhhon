package com.example.prm_shop.activities;

import android.annotation.SuppressLint;
import android.os.Bundle;
import android.view.View;
import android.widget.Button;
import android.widget.ImageView;
import android.widget.RadioButton;
import android.widget.RadioGroup;
import android.widget.TextView;
import android.widget.Toast;

import androidx.appcompat.app.AppCompatActivity;

import com.example.prm_shop.R;
import com.example.prm_shop.models.response.ProductResponse;
import com.squareup.picasso.Picasso;

public class ProductDetailActivity extends AppCompatActivity {

    private TextView unitInStock, productName, productPrice, productDescription, quantityTextView;
    private ImageView productImage;
    private Button addToCartButton, incrementButton, decrementButton;
    private RadioGroup sizeRadioGroup;
    private int quantity = 1;
    private ProductResponse product;

    @SuppressLint("SetTextI18n")
    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_product_detail);

        product = (ProductResponse) getIntent().getSerializableExtra("product");

        productName = findViewById(R.id.product_name);
        productPrice = findViewById(R.id.product_price);
        unitInStock = findViewById(R.id.product_unitInStock);
        productDescription = findViewById(R.id.product_description);
        quantityTextView = findViewById(R.id.quantity_text_view);
        productImage = findViewById(R.id.product_image);
        addToCartButton = findViewById(R.id.add_to_cart_button);
        incrementButton = findViewById(R.id.increment_button);
        decrementButton = findViewById(R.id.decrement_button);
        sizeRadioGroup = findViewById(R.id.size_radio_group);

        productName.setText(product.getProductName());
        productPrice.setText("Price: " + product.getUnitPrice());
        unitInStock.setText("Unit In Stock: " + product.getUnitsInStock());
        productDescription.setText("Description\n" + product.getDescription());
        Picasso.get().load(product.getImg()).into(productImage);

        incrementButton.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                quantity++;
                quantityTextView.setText(String.valueOf(quantity));
            }
        });

        decrementButton.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                if (quantity > 1) {
                    quantity--;
                    quantityTextView.setText(String.valueOf(quantity));
                }
            }
        });

        addToCartButton.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                int selectedSizeId = sizeRadioGroup.getCheckedRadioButtonId();
                if (selectedSizeId == -1) {
                    Toast.makeText(ProductDetailActivity.this, "Please select a size", Toast.LENGTH_SHORT).show();
                } else {
                    RadioButton selectedSizeRadioButton = findViewById(selectedSizeId);
                    String selectedSize = selectedSizeRadioButton.getText().toString();
                    Toast.makeText(ProductDetailActivity.this, "Added to cart: " + product.getProductName() + " - Size: " + selectedSize + " - Quantity: " + quantity, Toast.LENGTH_SHORT).show();
                }
            }
        });
    }
}