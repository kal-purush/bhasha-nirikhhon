package com.provizit.kioskcheckin.activities;

import static android.view.View.GONE;

import android.app.ProgressDialog;
import android.content.Context;
import android.content.Intent;
import android.content.SharedPreferences;
import android.os.Bundle;
import android.os.Handler;
import android.text.Html;
import android.view.KeyEvent;
import android.view.View;
import android.view.animation.AnimationSet;
import android.widget.Button;
import android.widget.CheckBox;
import android.widget.ImageView;
import android.widget.LinearLayout;
import android.widget.TextView;
import android.widget.Toast;

import androidx.appcompat.app.AppCompatActivity;
import androidx.core.content.res.ResourcesCompat;
import androidx.lifecycle.ViewModelProvider;

import com.bumptech.glide.Glide;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.provizit.kioskcheckin.R;
import com.provizit.kioskcheckin.config.Preferences;
import com.provizit.kioskcheckin.logins.VisitorLoginActivity;
import com.provizit.kioskcheckin.mvvm.ApiViewModel;
import com.provizit.kioskcheckin.services.Conversions;
import com.provizit.kioskcheckin.services.GetCVisitorDetailsModel;
import com.provizit.kioskcheckin.services.GetNdaActiveDetailsModel;
import com.provizit.kioskcheckin.utilities.IncompleteData;

import org.json.JSONException;
import org.json.JSONObject;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class NDAPermitActivity extends AppCompatActivity implements View.OnClickListener {

    ApiViewModel apiViewModel;

    ImageView company_logo;
    TextView txtName, txtMobile, txtEmail, txtDateTime, txtNDAName;
    LinearLayout linearMobile, linearCheckboxSelection, linaerEmail;
    String status_check = "0";
    CheckBox checkBox;
    Button btnAccept;
    ImageView back_image;

    GetNdaActiveDetailsModel ndamodel;
    GetCVisitorDetailsModel model;

    String comp_id = "";
    String inputValue = "";
    String valueType = "";
    String permitType = "";
    String ndaStatus = "";


    Float meeting_status;

    ProgressDialog progress;

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_permit_nda);

        comp_id = getIntent().getStringExtra("comp_id");
        inputValue = getIntent().getStringExtra("inputValue");
        valueType = getIntent().getStringExtra("valueType");
        permitType = getIntent().getStringExtra("permitType");
        ndaStatus = getIntent().getStringExtra("ndaStatus");

        btnAccept = findViewById(R.id.btnAccept);
        btnAccept.setTypeface(ResourcesCompat.getFont(this, R.font.arbfonts_dinnextttlrabic_medium));
        back_image = findViewById(R.id.back_image);
        linearMobile = findViewById(R.id.linearMobile);
        linearCheckboxSelection = findViewById(R.id.linearCheckboxSelection);
        checkBox = findViewById(R.id.checkbox);
        txtName = findViewById(R.id.txtName);
        txtMobile = findViewById(R.id.txtMobile);
        linaerEmail = findViewById(R.id.linaerEmail);
        txtEmail = findViewById(R.id.txtEmail);
        txtDateTime = findViewById(R.id.txtDateTime);
        txtNDAName = findViewById(R.id.txtNDAName);
        company_logo = findViewById(R.id.company_logo);

        progress = new ProgressDialog(this);
        progress.setTitle(getResources().getString(R.string.Loading));
        progress.setMessage(getResources().getString(R.string.whileloading));
        progress.setCancelable(true);
        progress.setCanceledOnTouchOutside(true);

        apiViewModel = new ViewModelProvider(NDAPermitActivity.this).get(ApiViewModel.class);

        SharedPreferences sharedPreferences = getSharedPreferences("MyPrefs", Context.MODE_PRIVATE);
        String VersionName = sharedPreferences.getString("VersionName", "1.0");
        TextView txtVersion = findViewById(R.id.txtVersion);
        txtVersion.setText(VersionName);

        DateTimeFormatter dtf = DateTimeFormatter.ofPattern("dd/MM/yyyy HH:mm:ss");
        LocalDateTime now = LocalDateTime.now();
        txtDateTime.setText(dtf.format(now));
        ndamodel = new GetNdaActiveDetailsModel();
        //company logo
        String c_Logo = Preferences.loadStringValue(getApplicationContext(), Preferences.company_Logo, "");
        if (c_Logo.equalsIgnoreCase("")) {
        } else {
            Glide.with(NDAPermitActivity.this).load(c_Logo)
                    .into(company_logo);
        }

        detailsSet();


        linearCheckboxSelection.setOnClickListener(this);
        checkBox.setOnClickListener(this);
        btnAccept.setOnClickListener(this);
        back_image.setOnClickListener(this);
    }

    //disable auto click action after scann
    @Override
    public boolean onKeyDown(int keyCode, KeyEvent event) {

        if (keyCode == KeyEvent.KEYCODE_ENTER) {
            // Barcode scanner has scanned a barcode, disable triggered items
            return true;
        } else {
            disableTriggeredItems();
        }
        return super.onKeyDown(keyCode, event);
    }

    //usb scanner
    @Override
    public boolean dispatchKeyEvent(KeyEvent event) {
        if (event.getAction() == KeyEvent.ACTION_DOWN) {
            switch (event.getKeyCode()) {
                case KeyEvent.KEYCODE_ENTER:
                    return true;
                case KeyEvent.KEYCODE_BACK:
                    Intent intent = new Intent(getApplicationContext(), VisitorLoginActivity.class);
                    startActivity(intent);
                    return true;
                default:
                    char keyChar = (char) event.getUnicodeChar();
                    return true;
            }
        }
        return super.dispatchKeyEvent(event);
    }

    private void disableTriggeredItems() {
        back_image.setFocusable(false);
        back_image.setFocusableInTouchMode(false);
        linearCheckboxSelection.setFocusable(false);
        linearCheckboxSelection.setFocusableInTouchMode(false);
        checkBox.setFocusable(false);
        checkBox.setFocusableInTouchMode(false);
        btnAccept.setFocusable(false);
        btnAccept.setFocusableInTouchMode(false);
        runthred();
    }

    private void runthred() {
        final Handler handler = new Handler();
        handler.postDelayed(() -> {

            finish();
            overridePendingTransition(0, 0);
            startActivity(getIntent());
            overridePendingTransition(0, 0);

            back_image.setFocusable(true);
            back_image.setFocusableInTouchMode(true);
            linearCheckboxSelection.setFocusable(true);
            linearCheckboxSelection.setFocusableInTouchMode(true);
            checkBox.setFocusable(true);
            checkBox.setFocusableInTouchMode(true);
            btnAccept.setFocusable(true);
            btnAccept.setFocusableInTouchMode(true);

        }, 500);
    }


    @Override
    public void onClick(View v) {
        switch (v.getId()) {
            case R.id.linearCheckboxSelection:
                AnimationSet animation1 = Conversions.animation();
                v.startAnimation(animation1);
                if (status_check.equalsIgnoreCase("0")) {
                    status_check = "1";
                    checkBox.setChecked(true);
                    btnAccept.setEnabled(true);
                    btnAccept.setBackgroundColor(btnAccept.getContext().getResources().getColor(R.color.colorPrimary));
                } else {
                    status_check = "0";
                    checkBox.setChecked(false);
                    btnAccept.setEnabled(false);
                    btnAccept.setBackgroundColor(btnAccept.getContext().getResources().getColor(R.color.light_gray));
                }
                break;
            case R.id.checkbox:
                if (checkBox.isChecked()) {
                    status_check = "1";
                    btnAccept.setEnabled(true);
                    btnAccept.setBackgroundColor(btnAccept.getContext().getResources().getColor(R.color.colorPrimary));
                } else {
                    status_check = "0";
                    btnAccept.setEnabled(false);
                    btnAccept.setBackgroundColor(btnAccept.getContext().getResources().getColor(R.color.light_gray));
                }
                break;
            case R.id.btnAccept:
                if (checkBox.isChecked()) {

                    if (permitType.equalsIgnoreCase("workpermit")) {
                        Intent intent = new Intent(getApplicationContext(), WorkPermitActivity.class);
                        intent.putExtra("comp_id", comp_id);
                        intent.putExtra("inputValue", inputValue);
                        intent.putExtra("valueType", valueType);
                        intent.putExtra("permitType", permitType);
                        intent.putExtra("ndaStatus", ndaStatus);
                        startActivity(intent);
                    } else {
                        Intent intent = new Intent(getApplicationContext(), MaterialPermitActivity.class);
                        intent.putExtra("comp_id", comp_id);
                        intent.putExtra("inputValue", inputValue);
                        intent.putExtra("valueType", valueType);
                        intent.putExtra("permitType", permitType);
                        intent.putExtra("ndaStatus", ndaStatus);
                        startActivity(intent);
                    }

                } else {
                    Toast.makeText(getApplicationContext(), "Please Accept the terms and conditions", Toast.LENGTH_SHORT).show();
                }
                break;
            case R.id.back_image:
                animation1 = Conversions.animation();
                v.startAnimation(animation1);
                Intent intent = new Intent(getApplicationContext(), VisitorLoginActivity.class);
                startActivity(intent);
                break;
        }
    }

    private void detailsSet() {

        TextView txtNote = (TextView) findViewById(R.id.txtNote);
        apiViewModel.getndafdetials(getApplicationContext(), "id", "active");
        apiViewModel.getResponseforNdaActiveDetails().observe(this, model1 -> {
            ndamodel = model1;
            try {
                if (ndamodel != null) {
                    try {
                        String htmlText = ndamodel.getItems().getContent();
                        Document doc = Jsoup.parse(htmlText);
                        String xmlText = doc.outerHtml();
                        txtNote.setText(Html.fromHtml(xmlText));
                        txtNDAName.setText(ndamodel.getItems().getName());
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });


        String emp_id = Preferences.loadStringValue(getApplicationContext(), Preferences.email_id, "");
        String location_id = Preferences.loadStringValue(getApplicationContext(), Preferences.location_id, "");
        apiViewModel.getcvisitordetails(getApplicationContext(), "comp_id", emp_id, inputValue, location_id);
        progress.show();
        apiViewModel.getResponseforCVisitor().observe(this, modelr -> {
            progress.dismiss();
            model = new GetCVisitorDetailsModel();
            model = modelr;
            try {
                if (model.getResult() == 200) {
                    Float visitor_status = model.getItems().getVisitorStatus();
                    if (visitor_status == 0) {
                        model.setIncomplete_data(new IncompleteData());

                        meeting_status = model.getItems().getMeetingStatus();
                        txtName.setText(model.getIncomplete_data().getName());

                        if (valueType.equalsIgnoreCase("email")) {
                            linearMobile.setVisibility(GONE);
                            linaerEmail.setVisibility(View.VISIBLE);
                            txtEmail.setText(inputValue);
                        } else {
                            linaerEmail.setVisibility(GONE);
                            txtMobile.setText(inputValue);
                        }

                    }
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
    }

    @Override
    public void onBackPressed() {
        super.onBackPressed();
        Intent intent = new Intent(getApplicationContext(), VisitorLoginActivity.class);
        startActivity(intent);
    }

}