package hungnp12.demo.daubep;

import android.content.Intent;
import android.os.Bundle;
import android.view.View;
import android.widget.ArrayAdapter;
import android.widget.Spinner;

import androidx.appcompat.app.AppCompatActivity;

import java.util.ArrayList;
import java.util.List;

public class CongThucNauAnActivity extends AppCompatActivity {


    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_cong_thuc_nau_an);
        Spinner spinner = findViewById(R.id.spinner) ;
        List<String> dataSrc = new ArrayList<>();
        dataSrc.add("1 người") ;
        dataSrc.add("2 người");
        dataSrc.add("3 Người") ;
        dataSrc.add("4 Người") ;

        ArrayAdapter<String> dataAdapter = new ArrayAdapter<>(this , android.R.layout.simple_spinner_item, dataSrc);
        dataAdapter.setDropDownViewResource(android.R.layout.simple_spinner_dropdown_item);
        spinner.setAdapter(dataAdapter);
    }

    public void chitietnauan(View view) {
        Intent intent = new Intent(this , ChiTietMonAnActivity.class);
        startActivity(intent);
    }



    public void clickToBack(View view){
        finish();
    }
}