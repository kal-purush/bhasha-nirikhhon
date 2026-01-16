package com.example.levents;

import android.annotation.SuppressLint;
import android.app.Dialog;
import android.content.Context;
import android.view.View;
import android.widget.Button;
import android.widget.EditText;

public class EditSanphamDialog extends Dialog {
    private EditText edt_tensanpham,edt_giasanpham,edt_motasanpham,edt_soluong,edt_anhsanpham;

    @SuppressLint("MissingInflatedId")
    public EditSanphamDialog(Context context) {
        super(context);
        setContentView(R.layout.dialog_suatt_sp);
        edt_tensanpham = findViewById(R.id.edt_tensanpham);
        edt_giasanpham = findViewById(R.id.edt_giasanpham);
        edt_soluong = findViewById(R.id.edt_soluong);
        edt_motasanpham = findViewById(R.id.edt_motasanpham);
        edt_anhsanpham = findViewById(R.id.edt_anhsanpham);

        Button buttonSave = findViewById(R.id.bt_updateSP);
        buttonSave.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                // Xử lý sự kiện khi người dùng nhấn nút lưu
                String tensanpham = edt_tensanpham.getText().toString();
                String giasanpham = edt_giasanpham.getText().toString();
                int soluong = Integer.parseInt(edt_soluong.getText().toString());
                String mota = edt_motasanpham.getText().toString();
                String anhsanpham = edt_anhsanpham.getText().toString();
                // Thực hiện lưu thông tin sản phẩm mới vào cơ sở dữ liệu hoặc thực hiện các thao tác cần thiết
                dismiss(); // Đóng dialog sau khi lưu
            }
        });
    }
}