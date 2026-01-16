package com.example.acortadorurl;

import android.os.Bundle;
import android.text.Editable;
import android.text.TextWatcher;
import android.util.Log;
import android.widget.Button;
import android.widget.EditText;
import android.widget.TextView;
import android.widget.Toast;
import androidx.appcompat.app.AppCompatActivity;
import com.google.firebase.auth.FirebaseAuth;
import com.google.firebase.auth.FirebaseUser;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.IOException;

import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;

public class PremiumActivity extends AppCompatActivity {

    private EditText etCardNumber, etExpiry, etCvv;
    private Button btnPay;
    private boolean isFormatting = false;
    private TextWatcher expiryTextWatcher;

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_premium);

        TextView tvBenefits = findViewById(R.id.tvBenefits);
        etCardNumber = findViewById(R.id.etCardNumber);
        etExpiry = findViewById(R.id.etExpiry);
        etCvv = findViewById(R.id.etCvv);
        btnPay = findViewById(R.id.btnPay);

        String benefits = "★ Beneficios Premium ★\n\n" +
                "• Intentos ilimitados para acortar URLs\n" +
                "• Sin Botón para hacerte premium\n" +
                "• Y ya we\n" +
                "• ¿Acaso querías más?";
        tvBenefits.setText(benefits);

        setupCardNumberFormatting();
        setupExpiryFormatting();
        setupValidationListener();

        btnPay.setOnClickListener(v -> upgradeToPremium());
    }

    private void setupCardNumberFormatting() {
        etCardNumber.addTextChangedListener(new TextWatcher() {
            @Override
            public void beforeTextChanged(CharSequence s, int start, int count, int after) {}

            @Override
            public void onTextChanged(CharSequence s, int start, int before, int count) {}

            @Override
            public void afterTextChanged(Editable s) {
                if (isFormatting) return;

                isFormatting = true;

                String original = s.toString().replaceAll("\\s", "");
                StringBuilder formatted = new StringBuilder();

                for (int i = 0; i < original.length(); i++) {
                    if (i > 0 && i % 4 == 0) {
                        formatted.append(" ");
                    }
                    formatted.append(original.charAt(i));
                }

                if (!s.toString().equals(formatted.toString())) {
                    etCardNumber.removeTextChangedListener(this);
                    etCardNumber.setText(formatted.toString());
                    etCardNumber.setSelection(formatted.length());
                    etCardNumber.addTextChangedListener(this);
                }

                isFormatting = false;
                validateFields();
            }
        });
    }

    private void setupExpiryFormatting() {
        expiryTextWatcher = new TextWatcher() {
            @Override
            public void beforeTextChanged(CharSequence s, int start, int count, int after) {}

            @Override
            public void onTextChanged(CharSequence s, int start, int before, int count) {
                if (s.length() == 2 && before == 0 && !s.toString().contains("/")) {
                    etExpiry.removeTextChangedListener(expiryTextWatcher);
                    etExpiry.setText(s + "/");
                    etExpiry.setSelection(3);
                    etExpiry.addTextChangedListener(expiryTextWatcher);
                }
            }

            @Override
            public void afterTextChanged(Editable s) {
                validateFields();
            }
        };

        etExpiry.addTextChangedListener(expiryTextWatcher);
    }

    private void setupValidationListener() {
        TextWatcher validationWatcher = new TextWatcher() {
            @Override
            public void beforeTextChanged(CharSequence s, int start, int count, int after) {}

            @Override
            public void onTextChanged(CharSequence s, int start, int before, int count) {}

            @Override
            public void afterTextChanged(Editable s) {
                validateFields();
            }
        };

        etCvv.addTextChangedListener(validationWatcher);
    }

    private void validateFields() {
        String cardNumber = etCardNumber.getText().toString().replaceAll("\\s", "");
        String expiry = etExpiry.getText().toString();
        String cvv = etCvv.getText().toString();

        boolean isCardValid = cardNumber.matches("^\\d{16}$");
        boolean isExpiryValid = expiry.matches("^\\d{2}/\\d{2}$");
        boolean isCvvValid = cvv.matches("^\\d{3,4}$");

        btnPay.setEnabled(isCardValid && isExpiryValid && isCvvValid);

        Log.d("VALIDATION", String.format(
                "Card: %b, Expiry: %b, CVV: %b",
                isCardValid, isExpiryValid, isCvvValid
        ));
    }

    private void upgradeToPremium() {
        Log.d("PremiumActivity", "Iniciando actualización a premium");

        FirebaseUser user = FirebaseAuth.getInstance().getCurrentUser();
        if (user == null || user.getEmail() == null) {
            Log.e("PremiumActivity", "Usuario no autenticado");
            showError("Debes iniciar sesión primero");
            return;
        }

        Log.d("PremiumActivity", "Usuario: " + user.getEmail());
        btnPay.setEnabled(false);
        btnPay.setText("Procesando...");

        new Thread(() -> {
            try {
                JSONObject json = new JSONObject();
                json.put("email", user.getEmail());
                String jsonStr = json.toString();
                Log.d("PremiumActivity", "JSON a enviar: " + jsonStr);

                RequestBody body = RequestBody.create(
                        jsonStr,
                        MediaType.parse("application/json")
                );

                String url = "https://apiurl.up.railway.app/update_user.php";
                Log.d("PremiumActivity", "URL: " + url);

                Request request = new Request.Builder()
                        .url(url)
                        .post(body)
                        .addHeader("Content-Type", "application/json")
                        .build();

                OkHttpClient client = new OkHttpClient();
                Log.d("PremiumActivity", "Enviando petición...");

                try (Response response = client.newCall(request).execute()) {
                    String responseData = response.body().string();
                    Log.d("PremiumActivity", "Respuesta cruda: " + responseData);

                    JSONObject jsonResponse = new JSONObject(responseData);
                    Log.d("PremiumActivity", "Respuesta parseada: " + jsonResponse.toString());

                    if (response.isSuccessful() && jsonResponse.getBoolean("success")) {
                        Log.d("PremiumActivity", "Actualización exitosa");
                        runOnUiThread(() -> {
                            getSharedPreferences("user_prefs", MODE_PRIVATE)
                                    .edit()
                                    .putBoolean("is_premium", true)
                                    .apply();
                            Toast.makeText(this, "¡Ahora eres Premium!", Toast.LENGTH_SHORT).show();
                            setResult(RESULT_OK);
                            finish();
                        });
                    } else {
                        String errorMsg = jsonResponse.optString("error",
                                jsonResponse.optString("message", "Error desconocido del servidor"));
                        Log.e("PremiumActivity", "Error del servidor: " + errorMsg);
                        showError(errorMsg);
                    }
                }
            } catch (JSONException e) {
                Log.e("PremiumActivity", "Error JSON", e);
                showError("Error procesando respuesta");
            } catch (IOException e) {
                Log.e("PremiumActivity", "Error de red", e);
                showError("Error de conexión: " + e.getMessage());
            } catch (Exception e) {
                Log.e("PremiumActivity", "Error inesperado", e);
                showError("Error inesperado");
            }
        }).start();
    }

    private void showError(String message) {
        runOnUiThread(() -> {
            btnPay.setEnabled(true);
            btnPay.setText("Actualizar a Premium");
            Toast.makeText(this, message, Toast.LENGTH_LONG).show();
        });
    }

    private void handleUpgradeError(String message) {
        runOnUiThread(() -> {
            btnPay.setEnabled(true);
            btnPay.setText("Actualizar a Premium");
            Toast.makeText(this, message, Toast.LENGTH_LONG).show();
            Log.e("UPGRADE_ERROR", message);
        });
    }
}