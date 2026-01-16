package com.cycle.single.task;


import android.app.Activity;
import android.os.Bundle;
import android.widget.SearchView;
import android.widget.TextView;

import butterknife.ButterKnife;
import butterknife.InjectView;
import utils.HTTPClient;


public class TranslateActivity extends Activity {
    @InjectView(R.id.translate_search)
    SearchView searchView;
    @InjectView(R.id.translate_text)
    TextView textView;

    @Override
    public void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_translate);
        ButterKnife.inject(this);
        searchView.setIconifiedByDefault(false);
        searchView.setSubmitButtonEnabled(true);
        searchView.setOnQueryTextListener(new SearchView.OnQueryTextListener() {
            @Override
            public boolean onQueryTextSubmit(String query) {
                translate(query);
                return true;
            }

            @Override
            public boolean onQueryTextChange(String newText) {
                return false;
            }
        });
    }

    private void translate(String string) {
        try {
            String result = HTTPClient.AnalyzingOfJson("http://fanyi.youdao.com/openapi.do" + "?keyfrom=" + "HomeworkHuster" + "&key=" + "1377657333" + "&type=" + "data" + "&doctype="
                    + "json" + "&type=" + "data" + "&version=" + "1.1" + "&q=" + string.trim(), TranslateActivity.this);
            textView.setText(result);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}