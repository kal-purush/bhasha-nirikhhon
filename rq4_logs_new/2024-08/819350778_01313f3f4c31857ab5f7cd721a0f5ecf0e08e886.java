package com.app.Regional_News;

import android.os.Bundle;
import android.util.Base64;
import android.util.Log;
import android.view.KeyEvent;
import android.view.MenuItem;
import android.view.View;
import android.widget.LinearLayout;
import android.widget.ProgressBar;
import android.widget.Toast;

import androidx.appcompat.app.AppCompatActivity;

import com.github.barteksc.pdfviewer.PDFView;

public class EnewspaperShowActivity extends AppCompatActivity {
    private PDFView pdfView;
    private ProgressBar progressBar;
    private LinearLayout lyt_not_found;
    private String getEnews_pdf_data, getEnewspaper_title;

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_enewspaper_show);

        // Retrieve data from Intent
        Bundle extras = getIntent().getExtras();
        if (extras != null) {
            getEnews_pdf_data = extras.getString("getEnews_pdf_data");
            getEnewspaper_title = extras.getString("getEnewspaper_title");
        }

        // Initialize views
        pdfView = findViewById(R.id.pdfView);
        progressBar = findViewById(R.id.progressBar);
        lyt_not_found = findViewById(R.id.lyt_not_found);

        // Set up ActionBar
        if (getSupportActionBar() != null) {
            getSupportActionBar().setDisplayHomeAsUpEnabled(true);
            getSupportActionBar().setTitle("eNewspaper " + getEnewspaper_title);
        }

        // Decode and display PDF
        if (getEnews_pdf_data != null) {
            showProgress(true);
            displayPDF(getEnews_pdf_data);
        } else {
            Toast.makeText(this, "No PDF data available", Toast.LENGTH_SHORT).show();
            lyt_not_found.setVisibility(View.VISIBLE);
        }
    }

    private void displayPDF(String base64PdfData) {
        try {
            byte[] pdfAsBytes = Base64.decode(base64PdfData, Base64.DEFAULT);
            pdfView.fromBytes(pdfAsBytes)
                    .enableSwipe(true)
                    .swipeHorizontal(false)
                    .enableDoubletap(true)
                    .defaultPage(0)
                    .load();
            lyt_not_found.setVisibility(View.GONE);
        } catch (Exception e) {
            Log.e("EnewspaperShowActivity", "Base64 decoding error: " + e.getMessage());
            Toast.makeText(EnewspaperShowActivity.this, "Error decoding PDF", Toast.LENGTH_SHORT).show();
            lyt_not_found.setVisibility(View.VISIBLE);
        } finally {
            showProgress(false);
        }
    }

    private void showProgress(boolean show) {
        if (show) {
            progressBar.setVisibility(View.VISIBLE);
            pdfView.setVisibility(View.GONE);
            lyt_not_found.setVisibility(View.GONE);
        } else {
            progressBar.setVisibility(View.GONE);
            pdfView.setVisibility(View.VISIBLE);
        }
    }

    @Override
    public boolean onKeyDown(int keyCode, KeyEvent event) {
        if (keyCode == KeyEvent.KEYCODE_BACK) {
            finish();
            return true;
        }
        return super.onKeyDown(keyCode, event);
    }

    @Override
    public boolean onOptionsItemSelected(MenuItem item) {
        if (item.getItemId() == android.R.id.home) {
            finish();
            return true;
        }
        return super.onOptionsItemSelected(item);
    }
}