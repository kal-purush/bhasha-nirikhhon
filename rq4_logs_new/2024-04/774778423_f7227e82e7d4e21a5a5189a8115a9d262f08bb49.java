package com.example.duan1chinhthuc.Activity_user;

import androidx.appcompat.app.AppCompatActivity;

import android.os.Bundle;
import android.view.View;
import android.widget.Button;
import android.widget.EditText;
import android.widget.Toast;

import com.example.duan1chinhthuc.DAO.ThuThuDao;
import com.example.duan1chinhthuc.R;
import com.example.duan1chinhthuc.mode.Donhang;
import com.example.duan1chinhthuc.mode.San_Pham;
import com.example.duan1chinhthuc.mode.nguoidung;

public class update_thongtingiaohang extends AppCompatActivity {
ThuThuDao dao;
nguoidung list;
    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_update_thongtingiaohang);

        EditText diachi = findViewById(R.id.diachigiaohang_update);
        EditText sdt = findViewById(R.id.sodienthoai_update);
        EditText ten = findViewById(R.id.hoten_update);


        Button luutt = findViewById(R.id.luthongtin);

        luutt.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
              String daichi1 = diachi.getText().toString();
                int sdt1 = Integer.parseInt(sdt.getText().toString());
                String ten1 = ten.getText().toString();


                nguoidung ls = new nguoidung(ten1,sdt1,daichi1);
                Boolean kt = dao.capnhaptongtin_giaohang(ls);


                if (daichi1.isEmpty() || ten1.isEmpty()) {
                    Toast.makeText(update_thongtingiaohang.this, "Không để trống", Toast.LENGTH_SHORT).show();
                    return;
                }else if(kt){

                }

            }
        });

    }
}