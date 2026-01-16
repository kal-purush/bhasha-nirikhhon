package com.example.backpackerlk.Activities;

import android.os.Bundle;
import android.text.Editable;
import android.text.TextWatcher;
import android.widget.EditText;

import androidx.appcompat.app.AppCompatActivity;
import androidx.core.graphics.Insets;
import androidx.core.view.ViewCompat;
import androidx.core.view.WindowInsetsCompat;

import com.example.backpackerlk.R;

public class EditSellerProfile extends AppCompatActivity {

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_edit_seller_profile);

        EditText editName = findViewById(R.id.edit_name);
        EditText editUsername = findViewById(R.id.edit_username);
        EditText editEmail = findViewById(R.id.edit_email);
        EditText editPassword = findViewById(R.id.edit_password);

        // Store original hint values in tags
        editName.setTag(editName.getHint());
        editUsername.setTag(editUsername.getHint());
        editEmail.setTag(editEmail.getHint());
        editPassword.setTag(editPassword.getHint());

        // Apply TextWatcher
        setHintVisibility(editName);
        setHintVisibility(editUsername);
        setHintVisibility(editEmail);
        setHintVisibility(editPassword);

        // Handle edge-to-edge layout
        ViewCompat.setOnApplyWindowInsetsListener(findViewById(R.id.main), (v, insets) -> {
            Insets systemBars = insets.getInsets(WindowInsetsCompat.Type.systemBars());
            v.setPadding(systemBars.left, systemBars.top, systemBars.right, systemBars.bottom);
            return insets;
        });
    }

    private void setHintVisibility(EditText editText) {
        editText.addTextChangedListener(new TextWatcher() {
            @Override
            public void beforeTextChanged(CharSequence s, int start, int count, int after) {
                // No action needed
            }

            @Override
            public void onTextChanged(CharSequence s, int start, int before, int count) {
                if (s.toString().trim().isEmpty()) {
                    editText.setHint(editText.getTag().toString());
                } else {
                    editText.setHint("");
                }
            }

            @Override
            public void afterTextChanged(Editable s) {
                // No action needed
            }
        });
    }
}