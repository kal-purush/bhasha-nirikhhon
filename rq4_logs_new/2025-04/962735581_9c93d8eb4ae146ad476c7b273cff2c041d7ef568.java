package com.example.btlandroid;

import android.content.Intent;
import android.graphics.Bitmap;
import android.net.Uri;
import android.os.Bundle;
import android.provider.MediaStore;
import android.util.Base64;
import android.widget.ArrayAdapter;
import android.widget.Button;
import android.widget.EditText;
import android.widget.ImageView;
import android.widget.Spinner;
import android.widget.TextView;
import android.widget.Toast;

import androidx.activity.EdgeToEdge;
import androidx.annotation.NonNull;
import androidx.appcompat.app.AppCompatActivity;

import com.example.btlandroid.Book.Book;
import com.google.firebase.database.DatabaseReference;
import com.google.firebase.database.FirebaseDatabase;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

public class AddAdminActivity extends AppCompatActivity {
    private EditText addName, addSl, addPrice, addNote;
    private Spinner addDanhmuc;
    private ImageView addImage, ivHome, ivBack, ivPerson;
    private Button addButton;
    private Uri imageUri;
    private static final int PICK_IMAGE_REQUEST = 1;

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        EdgeToEdge.enable(this);
        setContentView(R.layout.activity_add_admin);

        // Ánh xạ các view
        addName = findViewById(R.id.addName);
        addDanhmuc = findViewById(R.id.addDanhmuc);
        addSl = findViewById(R.id.addSl);
        addPrice = findViewById(R.id.addPrice);
        addNote = findViewById(R.id.addNote); // Ánh xạ trường mô tả
        addImage = findViewById(R.id.addImage);
        addButton = findViewById(R.id.addButton);
        ivHome = findViewById(R.id.ivHome);
        ivBack = findViewById(R.id.ivBack);
        ivPerson = findViewById(R.id.ivPerson);


        // Thiết lập Spinner
        String[] categories = {"Tiểu học", "THCS", "THPT", "Toeic", "Ielts"};
        ArrayAdapter<String> adapter = new ArrayAdapter<>(this,
                android.R.layout.simple_spinner_item, categories);
        adapter.setDropDownViewResource(android.R.layout.simple_spinner_dropdown_item);
        addDanhmuc.setAdapter(adapter);
        addDanhmuc.setSelection(0);

        // Xử lý nút quay lại
        ivBack.setOnClickListener(v -> finish());

        // Xử lý nút home
        ivHome.setOnClickListener(v -> {
            Intent intent = new Intent(AddAdminActivity.this, HomeAdminActivity.class);
            intent.setFlags(Intent.FLAG_ACTIVITY_CLEAR_TOP | Intent.FLAG_ACTIVITY_SINGLE_TOP);
            startActivity(intent);
            finish();
        });

        ivPerson.setOnClickListener(v -> {
            Intent intent = new Intent(AddAdminActivity.this, TaikhoanAdminActivity.class);
            intent.setFlags(Intent.FLAG_ACTIVITY_CLEAR_TOP | Intent.FLAG_ACTIVITY_SINGLE_TOP);
            startActivity(intent);
            finish();
        });

        // Xử lý chọn ảnh
        addImage.setOnClickListener(v -> {
            Intent intent = new Intent(Intent.ACTION_PICK, MediaStore.Images.Media.EXTERNAL_CONTENT_URI);
            startActivityForResult(intent, PICK_IMAGE_REQUEST);
        });

        // Xử lý nút Thêm
        addButton.setOnClickListener(v -> {
            String name = addName.getText().toString().trim();
            String category = addDanhmuc.getSelectedItem().toString();
            String note = addNote.getText().toString().trim();
            String quantityStr = addSl.getText().toString().trim();
            String priceStr = addPrice.getText().toString().trim();

            // Kiểm tra dữ liệu đầu vào
            if (name.isEmpty() || category.isEmpty() || quantityStr.isEmpty() || priceStr.isEmpty()) {
                Toast.makeText(AddAdminActivity.this, "Vui lòng nhập đầy đủ thông tin", Toast.LENGTH_SHORT).show();
                return;
            }

            int quantity;
            float price;
            try {
                quantity = Integer.parseInt(quantityStr);
                price = Float.parseFloat(priceStr);
            } catch (NumberFormatException e) {
                Toast.makeText(AddAdminActivity.this, "Số lượng hoặc giá không hợp lệ", Toast.LENGTH_SHORT).show();
                return;
            }

            // Chuyển đổi ảnh thành Base64
            String imageBase64 = "";
            if (imageUri != null) {
                try {
                    Bitmap bitmap = MediaStore.Images.Media.getBitmap(this.getContentResolver(), imageUri);
                    imageBase64 = bitmapToBase64(bitmap);
                } catch (IOException e) {
                    Toast.makeText(AddAdminActivity.this, "Không thể chuyển đổi ảnh", Toast.LENGTH_SHORT).show();
                    e.printStackTrace();
                    return;
                }
            }

            // Tạo đối tượng Book
            Book book = new Book();
            book.setName(name);
            book.setCategory(category);
            book.setNote(note);
            book.setQuantity(quantity);
            book.setPrice(price);
            book.setImage(imageBase64);

            // Lưu lên Firebase
            FirebaseDatabase database = FirebaseDatabase.getInstance();
            DatabaseReference myRef = database.getReference("book");
            String bookId = myRef.push().getKey(); // Tạo ID duy nhất
            book.setId((int) (System.currentTimeMillis() / 1000));

            myRef.child(bookId).setValue(book).addOnCompleteListener(task -> {
                if (task.isSuccessful()) {
                    Toast.makeText(AddAdminActivity.this, "Thêm sách thành công", Toast.LENGTH_SHORT).show();
                    finish(); // Quay lại HomeAdminActivity
                } else {
                    Toast.makeText(AddAdminActivity.this, "Thêm sách thất bại", Toast.LENGTH_SHORT).show();
                }
            });
        });
    }

    @Override
    protected void onActivityResult(int requestCode, int resultCode, Intent data) {
        super.onActivityResult(requestCode, resultCode, data);
        if (requestCode == PICK_IMAGE_REQUEST && resultCode == RESULT_OK && data != null) {
            imageUri = data.getData();
            addImage.setImageURI(imageUri);
        }
    }

    // Hàm chuyển Bitmap thành Base64
    private String bitmapToBase64(Bitmap bitmap) {
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        bitmap.compress(Bitmap.CompressFormat.JPEG, 100, byteArrayOutputStream);
        byte[] byteArray = byteArrayOutputStream.toByteArray();
        String encoded = Base64.encodeToString(byteArray, Base64.DEFAULT);
        return "data:image/jpeg;base64," + encoded; //
    }
}