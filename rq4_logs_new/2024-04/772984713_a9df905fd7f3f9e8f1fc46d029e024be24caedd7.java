package com.example.levents.Activity;

import android.content.Intent;
import android.content.SharedPreferences;
import android.os.Bundle;
import android.view.View;
import android.widget.Toast;

import androidx.appcompat.app.AppCompatActivity;

import com.example.levents.DAO.Nhanvien_DAO;
import com.example.levents.Model.Nhanvien;
import com.example.levents.R;
import com.example.levents.databinding.ActivityUpdateTtNvBinding;

import java.util.ArrayList;

public class UpdateTT_NV_Activity extends AppCompatActivity {
    ActivityUpdateTtNvBinding biding;
    Nhanvien_DAO dao;
    Nhanvien nguoiDung;
    private ArrayList<Nhanvien> list = new ArrayList<>();
    String tendangnhap1, matkhaucu1, matkhaumoi1, nhaplaimkmoi1, hoten1, email1, diachi1, sodienthoai1, matkhau, anh;
    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_update_tt_nv);
        biding = ActivityUpdateTtNvBinding.inflate(getLayoutInflater());
        setContentView(biding.getRoot());
        SharedPreferences preferences = getSharedPreferences("NHANVIEN",MODE_PRIVATE);
        int maTK = preferences.getInt("manhanvien",0);
        String tenDN = preferences.getString("tendangnhap", "");
        matkhau = preferences.getString("matkhau", "");
        String hoten = preferences.getString("hoten", "");
        String email = preferences.getString("email", "");
        String sodienthoai = preferences.getString("sodienthoai", "");
        String diachi = preferences.getString("diachi", "");
        String urlAnh = preferences.getString("anhnhanvien", "");
        dao = new Nhanvien_DAO(this);
        nguoiDung = dao.getNhanVienByMaTaiKhoan(maTK);
        biding.edtTenDangNhapDangKy.setText(tenDN);
        biding.edtNhapHoTen.setText(hoten);
        biding.edtNhapEmailDangKy.setText(email);
        biding.edtNhapDiaChiDangKy.setText(diachi);
        biding.edtNhapSDT.setText(sodienthoai);
        biding.edtNhapUrl.setText(urlAnh);
        biding.imgTroVeDangNhap.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View view) {
                startActivity(new Intent(UpdateTT_NV_Activity.this,Main_Activity_Nhanvien.class));
            }
        });
        biding.btnDongY.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View view) {
                validTenDangNhap();
                validMatKhauCu();
                validNhapLaiMatKhauMoi();
                validEmail();
                validDiaChi();
                validHoTen();
                validSoDienThoai();
                validUrl();
                if (biding.edtTenDangNhapDangKy.getError() == null &&
                        biding.edmatKhau.getError() == null &&
                        biding.edmatKhauMoi.getError() == null &&
                        biding.edtNhapLaiPassMoi.getError() == null &&
                        biding.edtNhapHoTen.getError() == null &&
                        biding.edtNhapEmailDangKy.getError() == null &&
                        biding.edtNhapDiaChiDangKy.getError() == null &&
                        biding.edtNhapSDT.getError() == null &&
                        biding.edtNhapUrl.getError() == null) {
                    if (matkhaucu1.equals(matkhau)) {
                        nguoiDung.setTendangnhap(tendangnhap1);
                        nguoiDung.setMatkhau(matkhaumoi1);
                        nguoiDung.setHoten(hoten1);
                        nguoiDung.setEmail(email1);
                        nguoiDung.setDiachi(diachi1);
                        nguoiDung.setAnhnhanvien(urlAnh);
                        nguoiDung.setSodienthoai(sodienthoai1);
                        boolean result = dao.updatenhanvien(nguoiDung);
                        if (result) {
                            list.clear();
                            list = dao.getAllNhanVien();
                            Intent intent = new Intent(UpdateTT_NV_Activity.this, Login_Activity.class);
                            startActivity(intent);
                            Toast.makeText(UpdateTT_NV_Activity.this, "Đổi thông tin thành công", Toast.LENGTH_SHORT).show();
                        } else {
                            // Đăng ký thất bại
                            Toast.makeText(UpdateTT_NV_Activity.this, "Đăng ký thất bại", Toast.LENGTH_SHORT).show();
                        }
                    } else {
                        biding.edmatKhau.setError("mật khẩu cũ không trùng khớp");
                    }
                }
            }
        });

    }
    private void validTenDangNhap() {
        tendangnhap1 = biding.edtTenDangNhapDangKy.getText().toString();
        if (tendangnhap1.isEmpty()) {
            biding.edtTenDangNhapDangKy.setError("Vui lòng nhập tên đăng nhập");
        } else {
            biding.edtTenDangNhapDangKy.setError(null);
        }
    }

    private void validMatKhauCu() {
        matkhaucu1 = biding.edmatKhau.getText().toString();
        if (matkhaucu1.isEmpty()) {
            biding.edmatKhau.setError("Vui lòng nhập lại mật khẩu");

        } else if (!matkhaucu1.equals(matkhau)) {
            biding.edmatKhau.setError("Mật khẩu không đúng");
        } else {
            biding.edmatKhau.setError(null);
        }
    }



    private void validNhapLaiMatKhauMoi() {
        nhaplaimkmoi1 = biding.edtNhapLaiPassMoi.getText().toString();
        matkhaumoi1 = biding.edmatKhauMoi.getText().toString();
        if (nhaplaimkmoi1.isEmpty()) {
            biding.edtNhapLaiPassMoi.setError("Vui lòng nhập lại mật khẩu");
        } else if (matkhaumoi1.isEmpty()) {
            biding.edmatKhauMoi.setError("Vui lòng nhập mật khẩu mới");
        } else if (!nhaplaimkmoi1.equals(matkhaumoi1)) {
            biding.edtNhapLaiPassMoi.setError("Mật khẩu không trùng nhau");
        } else {
            biding.edtNhapLaiPassMoi.setError(null);
            biding.edmatKhauMoi.setError(null);
        }
    }

    private void validHoTen() {
        hoten1 = biding.edtNhapHoTen.getText().toString();
        if (hoten1.isEmpty()) {
            biding.edtNhapHoTen.setError("Vui lòng nhập họ tên");
        } else {
            biding.edtNhapHoTen.setError(null);
        }
    }

    private void validEmail() {
        email1 = biding.edtNhapEmailDangKy.getText().toString();
        if (email1.isEmpty()) {
            biding.edtNhapEmailDangKy.setError("Vui lòng nhập email");
        } else if (!isValidEmail(email1)) {
            biding.edtNhapEmailDangKy.setError("Email không hợp lệ");
        } else {
            biding.edtNhapEmailDangKy.setError(null);
        }
    }

    private void validDiaChi() {
        diachi1 = biding.edtNhapDiaChiDangKy.getText().toString();
        if (diachi1.isEmpty()) {
            biding.edtNhapDiaChiDangKy.setError("Vui lòng nhập địa chỉ");
        } else {
            biding.edtNhapDiaChiDangKy.setError(null);
        }
    }

    private void validSoDienThoai() {
        sodienthoai1 = biding.edtNhapSDT.getText().toString();
        if (sodienthoai1.isEmpty()) {
            biding.edtNhapSDT.setError("Vui lòng nhập số điện thoại");
        } else if (!isValidPhoneNumber(sodienthoai1)) {
            biding.edtNhapSDT.setError("Số điện thoại không hợp lệ");
        } else {
            biding.edtNhapSDT.setError(null);
        }
    }

    private void validUrl() {
        anh = biding.edtNhapUrl.getText().toString();
        if (anh.isEmpty()) {
            biding.edtNhapUrl.setError("Vui lòng nhập địa chỉ");
        } else {
            biding.edtNhapUrl.setError(null);
        }
    }

    private boolean isValidPhoneNumber(String phoneNumber) {
        String regex = "0\\d{9}";
        return phoneNumber.matches(regex);
    }

    // Hàm kiểm tra định dạng email
    private boolean isValidEmail(String email) {
        String regex = "[a-zA-Z0-9._-]+@[a-z]+\\.+[a-z]+(\\.+[a-z]+)?";
        return email.matches(regex);
    }
}