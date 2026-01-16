package hcmute.edu.vn.myfoody;

import androidx.annotation.NonNull;
import androidx.annotation.Nullable;
import androidx.appcompat.app.AppCompatActivity;
import androidx.core.app.ActivityCompat;

import android.Manifest;
import android.content.Intent;
import android.content.pm.PackageManager;
import android.graphics.Bitmap;
import android.graphics.BitmapFactory;
import android.graphics.drawable.BitmapDrawable;
import android.net.Uri;
import android.os.Bundle;
import android.view.View;
import android.widget.Button;
import android.widget.EditText;
import android.widget.ImageButton;
import android.widget.ImageView;
import android.widget.Toast;

import java.io.ByteArrayOutputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;

public class AddFoodActivity extends AppCompatActivity {

    ImageView imageViewFood;
    ImageButton imageButtonChoosePhoto;
    ImageButton imageButtonBackAddFood;
    EditText editTextFoodName;
    EditText editTextPrice;
    Button buttonThem;

    Integer StoreId;

    final int REQUEST_CODE_GALLERY = 999;

    Database database;

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_add_food);

        StoreId = getIntent().getIntExtra("StoreId", 0);

        database = new Database(AddFoodActivity.this, "foody.sqlite", null, 1);

        imageViewFood = (ImageView) findViewById(R.id.imageViewFood);
        imageButtonBackAddFood = (ImageButton) findViewById(R.id.imgBackAddFood);
        imageButtonChoosePhoto = (ImageButton) findViewById(R.id.imgChoosePhoto);
        editTextFoodName = (EditText) findViewById(R.id.editTextFoodName);
        editTextPrice = (EditText) findViewById(R.id.editTextPrice);
        buttonThem = (Button) findViewById(R.id.buttonAddFood);

        imageButtonBackAddFood.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View view) {
                finish();
            }
        });

        imageButtonChoosePhoto.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View view) {
                ActivityCompat.requestPermissions(
                        AddFoodActivity.this,
                        new String[] {Manifest.permission.READ_EXTERNAL_STORAGE},
                        REQUEST_CODE_GALLERY
                );
            }
        });

        buttonThem.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View view) {
                String foodName = editTextFoodName.getText().toString().trim();
                String foodPrice = editTextPrice.getText().toString().trim();
                try {
                    if (foodName.equals("") == false && foodPrice.equals("") == false) {
                        database.insertFood(
                                foodName,
                                imageViewToByte(imageViewFood),
                                Float.valueOf(foodPrice),
                                StoreId
                        );
                        imageViewFood.setImageResource(R.mipmap.ic_launcher);
                        editTextFoodName.setText("");
                        editTextPrice.setText("");
                    } else {
                        Toast.makeText(AddFoodActivity.this, "Cần nhập đủ thông tin", Toast.LENGTH_SHORT).show();
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });
    }

    @Override
    public void onRequestPermissionsResult(int requestCode, @NonNull String[] permissions, @NonNull int[] grantResults) {
        if (requestCode == REQUEST_CODE_GALLERY) {
            if (grantResults.length > 0 && grantResults[0] == PackageManager.PERMISSION_GRANTED) {
                Intent intent = new Intent(Intent.ACTION_PICK);
                intent.setType("image/*");
                startActivityForResult(intent, REQUEST_CODE_GALLERY);
            } else {
                Toast.makeText(getApplicationContext(), "You don't have permission to access file location!", Toast.LENGTH_SHORT).show();
            }
            return;
        }
        super.onRequestPermissionsResult(requestCode, permissions, grantResults);
    }

    @Override
    protected void onActivityResult(int requestCode, int resultCode, @Nullable Intent data) {

        if (requestCode == REQUEST_CODE_GALLERY && resultCode == RESULT_OK && data != null) {

            Uri uri = data.getData();

            try {
                InputStream inputStream = getContentResolver().openInputStream(uri);

                Bitmap bitmap = BitmapFactory.decodeStream(inputStream);
                imageViewFood.setImageBitmap(bitmap);
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            }
        }
        super.onActivityResult(requestCode, resultCode, data);
    }

    private byte[] imageViewToByte(ImageView image) {
        Bitmap bitmap = ((BitmapDrawable)image.getDrawable()).getBitmap();
        ByteArrayOutputStream stream = new ByteArrayOutputStream();
        bitmap.compress(Bitmap.CompressFormat.PNG, 100, stream);
        byte[] byteArray = stream.toByteArray();
        return byteArray;
    }
}