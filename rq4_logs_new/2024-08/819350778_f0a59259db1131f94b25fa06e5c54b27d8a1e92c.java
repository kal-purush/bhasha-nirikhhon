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

import com.app.Regional_News.data.Pdf_data;
import com.app.Regional_News.data.Pdf_listdata;
import com.app.Regional_News.extra.BaseApiService;
import com.app.Regional_News.extra.NetworkUtils;
import com.app.Regional_News.extra.UtilsApi;
import com.github.barteksc.pdfviewer.PDFView;

import java.util.ArrayList;
import java.util.List;

import retrofit2.Call;
import retrofit2.Callback;
import retrofit2.Response;

public class EnewspaperActivity extends AppCompatActivity {

    private PDFView pdfView;
    private ProgressBar progressBar;
    private LinearLayout lyt_not_found;
    private BaseApiService apiService;

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_enewspaper);

        // Initialize views
        pdfView = findViewById(R.id.pdfView);
        progressBar = findViewById(R.id.progressBar);
        lyt_not_found = findViewById(R.id.lyt_not_found);

        // Set up ActionBar
        if (getSupportActionBar() != null) {
            getSupportActionBar().setDisplayHomeAsUpEnabled(true);
            getSupportActionBar().setTitle("eNewspaper");
        }

        // Get API service instance
        apiService = UtilsApi.getAPIService(); // Ensure this method is properly defined

        // Check network connectivity and fetch PDF
        if (NetworkUtils.isConnected(this)) {
            showProgress(true);
            fetchAndDisplayPDF();
        } else {
            Toast.makeText(this, getString(R.string.conne_msg1), Toast.LENGTH_SHORT).show();
        }
    }

    private void fetchAndDisplayPDF() {
        Call<Pdf_data> call = apiService.fetchAllPdfs("fetch_all_pdfs");

        call.enqueue(new Callback<Pdf_data>() {
            @Override
            public void onResponse(Call<Pdf_data> call, Response<Pdf_data> response) {
                showProgress(false); // Hide progress bar

                if (response.isSuccessful() && response.body() != null) {
                    Pdf_data pdfResponse = response.body();
                    Log.d("EnewspaperActivity", "Response Body: " + pdfResponse.toString());
                    Log.d("EnewspaperActivity", "Status: " + pdfResponse.getStatus());
                    Log.d("EnewspaperActivity", "Message: " + pdfResponse.getMsg());

                    if (pdfResponse.getStatus().equals("1")) {
                        List<Pdf_listdata> pdfs = pdfResponse.getPdf_listdata();
                        Log.d("EnewspaperActivity", "PDF List Size: " + (pdfs != null ? pdfs.size() : 0));

                        if (pdfs != null && !pdfs.isEmpty()) {
                            Pdf_listdata pdfData = pdfs.get(0);
                            Log.d("EnewspaperActivity", "PDF Data (Base64): " + pdfData.getEnews_pdf_data());

                            try {
                                byte[] pdfAsBytes = Base64.decode(pdfData.getEnews_pdf_data(), Base64.DEFAULT);
                                pdfView.fromBytes(pdfAsBytes)
                                        .enableSwipe(true)
                                        .swipeHorizontal(false)
                                        .enableDoubletap(true)
                                        .defaultPage(0)
                                        .load();
                                lyt_not_found.setVisibility(View.GONE);
                            } catch (Exception e) {
                                Log.e("EnewspaperActivity", "Base64 decoding error: " + e.getMessage());
                                Toast.makeText(EnewspaperActivity.this, "Error decoding PDF", Toast.LENGTH_SHORT).show();
                                lyt_not_found.setVisibility(View.VISIBLE);
                            }
                        } else {
                            lyt_not_found.setVisibility(View.VISIBLE);
                            Toast.makeText(EnewspaperActivity.this, "No PDFs found", Toast.LENGTH_SHORT).show();
                        }
                    } else {
                        Toast.makeText(EnewspaperActivity.this, "Error: " + pdfResponse.getMsg(), Toast.LENGTH_SHORT).show();
                    }
                } else {
                    Toast.makeText(EnewspaperActivity.this, "Failed to fetch PDF", Toast.LENGTH_SHORT).show();
                }
            }

            @Override
            public void onFailure(Call<Pdf_data> call, Throwable t) {
                showProgress(false); // Hide progress bar
                Log.e("EnewspaperActivity", "Retrofit error: " + t.getMessage());
                Toast.makeText(EnewspaperActivity.this, "Error: " + t.getMessage(), Toast.LENGTH_SHORT).show();
            }
        });
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