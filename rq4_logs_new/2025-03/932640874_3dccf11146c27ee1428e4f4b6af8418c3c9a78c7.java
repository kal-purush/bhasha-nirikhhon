package dev.frilly.locket.activities;

import android.app.DatePickerDialog;
import android.os.Bundle;
import android.text.Editable;
import android.text.TextWatcher;
import android.util.Log;
import android.view.View;
import android.widget.Button;
import android.widget.DatePicker;
import android.widget.EditText;
import android.widget.ImageButton;
import android.widget.TextView;

import androidx.annotation.NonNull;
import androidx.annotation.Nullable;
import androidx.appcompat.app.AppCompatActivity;

import com.google.android.material.textfield.TextInputLayout;

import org.json.JSONException;
import org.json.JSONObject;

import java.io.IOException;
import java.sql.Date;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.LocalDate;
import java.time.format.DateTimeParseException;
import java.util.Calendar;
import java.util.Locale;
import java.util.regex.Pattern;

import dev.frilly.locket.Authentication;
import dev.frilly.locket.Constants;
import dev.frilly.locket.R;
import dev.frilly.locket.utils.AndroidUtil;
import okhttp3.Call;
import okhttp3.Callback;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;

/**
 * The activity that is started when the "Edit Info" button is clicked on
 * the profile screen.
 */
public class ProfileEditInfoActivity extends AppCompatActivity implements DatePickerDialog.OnDateSetListener {

    private static final Pattern DATE_PATTERN = Pattern.compile("\\d{4}-\\d{2}-\\d{2}");
    private final Calendar calendar = Calendar.getInstance();

    private TextInputLayout layoutFieldDob;
    private EditText fieldUsername;
    private EditText fieldPassword;
    private EditText fieldDob;

    private TextView textError;
    private ImageButton buttonBack;
    private Button buttonContinue;

    @Override
    protected void onCreate(@Nullable Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.profile_edit_screen);
        AndroidUtil.applyInsets(this, R.id.layout_outer);

        layoutFieldDob = findViewById(R.id.layout_field_dob);
        fieldUsername = findViewById(R.id.field_username);
        fieldPassword = findViewById(R.id.field_password);
        fieldDob = findViewById(R.id.field_dob);
        textError = findViewById(R.id.text_error);
        buttonBack = findViewById(R.id.button_back);
        buttonContinue = findViewById(R.id.button_continue);

        calendar.setTime(Date.from(Instant.now()));

        buttonBack.setOnClickListener(e -> getOnBackPressedDispatcher().onBackPressed());
        buttonContinue.setOnClickListener(this::onContinueClick);
        fieldDob.addTextChangedListener(new DateTextWatcher());
        layoutFieldDob.setEndIconOnClickListener(this::onEndIconClick);
    }

    private void onEndIconClick(final View view) {
        final var dialog = new DatePickerDialog(this, this, calendar.get(Calendar.YEAR),
                calendar.get(Calendar.MONTH), calendar.get(Calendar.DAY_OF_MONTH));
        dialog.show();
    }

    private void onContinueClick(final View view) {
        textError.setText("");

        if (!fieldDob.getText().toString().isBlank() && !DATE_PATTERN.matcher(fieldDob.getText()).matches()) {
            textError.setText(R.string.error_birthdate_invalid);
            return;
        }

        try {
            final var body = new JSONObject();
            if (!fieldUsername.getText().toString().isBlank()) {
                body.put("username", fieldUsername.getText().toString().trim());
            }
            if (!fieldPassword.getText().toString().isBlank()) {
                body.put("password", fieldPassword.getText().toString().trim());
            }
            if (!fieldDob.getText().toString().isBlank()) {
                body.put("birthdate", fieldDob.getText().toString().trim());
            }

            final var req = new Request.Builder()
                    .url(Constants.BACKEND_URL + "profiles")
                    .header("Authorization", "Bearer " + Authentication.getToken(this))
                    .put(RequestBody.create(body.toString(), Constants.JSON))
                    .build();

            Constants.HTTP_CLIENT.newCall(req).enqueue(new UpdateProfilesCallback());
        } catch (Exception e) {
            Log.e("ProfileEditInfoActivity", e.getMessage(), e);
        }
    }

    @Override
    public void onDateSet(DatePicker datePicker, int y, int m, int d) {
        calendar.set(Calendar.YEAR, y);
        calendar.set(Calendar.MONTH, m);
        calendar.set(Calendar.DAY_OF_MONTH, d);

        final var formatter = new SimpleDateFormat("yyyy-MM-dd", Locale.getDefault());
        fieldDob.setText(formatter.format(calendar.getTime()));
    }

    private class DateTextWatcher implements TextWatcher {

        @Override
        public void beforeTextChanged(CharSequence charSequence, int i, int i1, int i2) {
            // Left blank
        }

        @Override
        public void onTextChanged(CharSequence charSequence, int i, int i1, int i2) {
            // Left blank
        }

        @Override
        public void afterTextChanged(Editable editable) {
            try {
                final var date = LocalDate.parse(editable);
                calendar.set(Calendar.YEAR, date.getYear());
                calendar.set(Calendar.MONTH, date.getMonthValue());
                calendar.set(Calendar.DAY_OF_MONTH, date.getDayOfMonth());
            } catch (DateTimeParseException exception) {
                // Intentionally left blank.
                textError.setText(R.string.error_birthdate_invalid);
            }
        }

    }

    private class UpdateProfilesCallback implements Callback {

        @Override
        public void onFailure(@NonNull Call call, @NonNull IOException e) {
            Log.e("UpdateProfileCallback", e.getMessage(), e);
            runOnUiThread(() -> textError.setText(R.string.error_unknown));
        }

        @Override
        public void onResponse(@NonNull Call call, @NonNull Response response) throws IOException {
            switch (response.code()) {
                case 409:
                    runOnUiThread(() -> textError.setText(R.string.error_username_taken));
                    break;
                case 400:
                    runOnUiThread(() -> textError.setText(R.string.error_invalid));
                    break;
                case 200:
                    try {
                        final var body = new JSONObject(response.body().string());
                        Authentication.saveUserData(ProfileEditInfoActivity.this, body);
                        runOnUiThread(() -> getOnBackPressedDispatcher().onBackPressed());
                    } catch (JSONException e) {
                        Log.e("UpdateProfileCallback", e.getMessage(), e);
                    }
            }
        }

    }

}