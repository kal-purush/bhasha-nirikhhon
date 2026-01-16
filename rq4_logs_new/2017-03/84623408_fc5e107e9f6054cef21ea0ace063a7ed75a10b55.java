package com.anca.exchange_rate.activity;

import android.os.Bundle;
import android.support.design.widget.FloatingActionButton;
import android.support.v7.app.AppCompatActivity;
import android.support.v7.widget.Toolbar;
import android.util.Log;
import android.view.View;
import android.widget.Button;
import android.widget.EditText;
import android.widget.Toast;

import com.afollestad.materialdialogs.MaterialDialog;
import com.anca.exchange_rate.R;
import com.anca.exchange_rate.api.ERResponse;
import com.anca.exchange_rate.api.ExchangeRateService;
import com.anca.exchange_rate.utils.ApiUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import retrofit2.Call;
import retrofit2.Callback;
import retrofit2.Response;
import retrofit2.Retrofit;
import retrofit2.converter.simplexml.SimpleXmlConverterFactory;

public class ExchangeActivity extends AppCompatActivity {

    private Button btnInput;
    private Button btnOutput;
    private EditText edtInput;
    private EditText edtOutput;

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_exchange);
        Toolbar toolbar = (Toolbar) findViewById(R.id.toolbar);
        toolbar.setNavigationIcon(R.drawable.ic_clear_black_24dp);
        setSupportActionBar(toolbar);

        btnInput = (Button) findViewById(R.id.btn_input);
        btnOutput = (Button) findViewById(R.id.btn_output);
        edtInput = (EditText) findViewById(R.id.edt_input);
        edtOutput = (EditText) findViewById(R.id.edt_output);

        toolbar.setNavigationOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                onBackPressed();
                overridePendingTransition(R.anim.no_change_anim, R.anim.close_anim);
            }

        });

        btnInput.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                showChangeCurrencyUnitDialog(btnInput, "Choose input currency");
            }
        });

        btnOutput.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                showChangeCurrencyUnitDialog(btnOutput, "Choose output currency");
            }
        });

        FloatingActionButton fab = (FloatingActionButton) findViewById(R.id.fab);
        fab.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View view) {
                loadExchangeRate();
            }
        });
    }

    private void showChangeCurrencyUnitDialog(final Button btn, String nofiText) {
        List<String> lstCurrencyUnit = new ArrayList<>();
        String[] lstCurrency = getResources().getStringArray(R.array.lst_currency);
        lstCurrencyUnit.addAll(Arrays.asList(lstCurrency));

        MaterialDialog.ListCallbackSingleChoice callback = new MaterialDialog.ListCallbackSingleChoice() {
            @Override
            public boolean onSelection(MaterialDialog dialog, View itemView, int which, CharSequence text) {
                if (text != null) {
                    btn.setText(text);
                }
                return true;
            }
        };

        new MaterialDialog.Builder(this)
                .title(nofiText)
                .items(lstCurrencyUnit)
                .itemsCallbackSingleChoice(-1, callback)
                .positiveText("Change")
                .show();

    }

    private void loadExchangeRate() {

        final MaterialDialog dialog = new MaterialDialog.Builder(this)
                .title("Loading data...")
                .progress(true, 0)
                .progressIndeterminateStyle(true)
                .show();

        Retrofit retrofit = new Retrofit.Builder()
                .baseUrl(ExchangeRateService.YAHOO_BASE_URL)
                .addConverterFactory(SimpleXmlConverterFactory.create())
                .build();
        ExchangeRateService service = retrofit.create(ExchangeRateService.class);

        List<String> toCurrency = new ArrayList<>();
        toCurrency.add(btnOutput.getText().toString());

        String query = ApiUtils.buildQuery(btnInput.getText().toString(), toCurrency);
        final Call<ERResponse> lstExchangeRate = service.getExchangeRate(query, ExchangeRateService.YAHOO_ENV);

        lstExchangeRate.enqueue(new Callback<ERResponse>() {
            @Override
            public void onResponse(Call<ERResponse> call, Response<ERResponse> response) {
                Log.d("RESPONSE", "response OK");
                double output = Double.valueOf(edtInput.getText().toString()) *
                        response.body().getLstExchangeRate().get(0).getRate();
                edtOutput.setText(String.valueOf(output));
                dialog.dismiss();
            }

            @Override
            public void onFailure(Call<ERResponse> call, Throwable t) {
                Log.d("RESPONSE", "response failure");
            }
        });
    }

}