package dev.frilly.locket.activities;

import android.content.Intent;
import android.os.Bundle;
import android.util.Log;
import android.view.View;
import android.widget.Button;
import android.widget.EditText;
import android.widget.ImageButton;
import android.widget.TextView;

import androidx.annotation.NonNull;
import androidx.annotation.Nullable;
import androidx.appcompat.app.AppCompatActivity;

import org.json.JSONException;
import org.json.JSONObject;

import java.io.IOException;

import dev.frilly.locket.Authentication;
import dev.frilly.locket.Constants;
import dev.frilly.locket.R;
import okhttp3.Call;
import okhttp3.Callback;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;

/**
 * The activity to change email after authentication.
 */
public class ChangeEmailActivity extends AppCompatActivity {

    private ImageButton buttonBack;
    private EditText fieldEmail;
    private TextView textError;
    private Button buttonContinue;

    @Override
    protected void onCreate(@Nullable Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.change_email_screen);

        buttonBack = findViewById(R.id.button_back);
        fieldEmail = findViewById(R.id.field_email);
        textError = findViewById(R.id.text_error);
        buttonContinue = findViewById(R.id.button_continue);

        buttonBack.setOnClickListener(this::onBack);
        buttonContinue.setOnClickListener(this::onContinue);
    }

    private void onBack(final View view) {
        getOnBackPressedDispatcher().onBackPressed();
    }

    private void onContinue(final View view) {
        if (fieldEmail.getText().toString().isEmpty()) {
            textError.setText(R.string.error_email_empty);
            return;
        }

        try {
            final var body = new JSONObject();
            body.put("email", fieldEmail.getText());

            final var req = new Request.Builder()
                    .url(Constants.BACKEND_URL + "profiles")
                    .header("Authorization", "Bearer " + Authentication.getToken(this))
                    .put(RequestBody.create(body.toString(), Constants.JSON))
                    .build();

            Constants.HTTP_CLIENT.newCall(req).enqueue(new PutEmailCallback());
        } catch (Exception e) {
            textError.setText(R.string.error_unknown);
            Log.e("ChangeEmailActivity", e.getMessage(), e);
        }
    }

    private class PutEmailCallback implements Callback {

        @Override
        public void onFailure(@NonNull Call call, @NonNull IOException e) {
            runOnUiThread(() -> {
                textError.setText(R.string.error_unknown);
                Log.e("PutEmailCallback", e.getMessage(), e);
            });
        }

        private void handleUnsuccessful(final int code) {
            switch (code) {
                case 400:
                    textError.setText(R.string.error_email_invalid);
                    break;
                case 409:
                    textError.setText(R.string.error_email_taken);
                    break;
                case 401:
                    final var intent = new Intent(ChangeEmailActivity.this, WelcomeActivity.class);
                    intent.setFlags(Intent.FLAG_ACTIVITY_NO_HISTORY);
                    startActivity(intent);
                    break;
            }
        }

        @Override
        public void onResponse(@NonNull Call call, @NonNull Response response) throws IOException {
            if (response.isSuccessful()) {
                try {
                    final var body = response.body();
                    final var jsonBody = new JSONObject(body.string());

                    final var prof = Authentication.getUserData(ChangeEmailActivity.this);
                    prof.put("email", jsonBody.get("email"));
                    Authentication.saveUserData(ChangeEmailActivity.this, prof);

                    runOnUiThread(() -> onBack(null));
                } catch (JSONException ex) {
                    Log.e("PutEmailCallback", ex.getMessage(), ex);
                    runOnUiThread(() -> textError.setText(R.string.error_unknown));
                }

                return;
            }

            runOnUiThread(() -> handleUnsuccessful(response.code()));
        }

    }

}