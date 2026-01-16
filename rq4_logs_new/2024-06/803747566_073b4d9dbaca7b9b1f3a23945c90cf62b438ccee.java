package fcu.midterm.bookstore;

import androidx.annotation.Nullable;
import androidx.appcompat.app.AppCompatActivity;

import android.content.ContentResolver;
import android.content.Intent;
import android.net.Uri;
import android.os.Bundle;
import android.view.View;
import android.webkit.MimeTypeMap;
import android.widget.Button;
import android.widget.EditText;
import android.widget.ImageView;
import android.widget.Toast;

import com.google.android.gms.tasks.OnSuccessListener;
import com.google.firebase.database.DatabaseReference;
import com.google.firebase.database.FirebaseDatabase;
import com.google.firebase.storage.FirebaseStorage;
import com.google.firebase.storage.StorageReference;
import com.google.firebase.storage.UploadTask;

import java.util.Date;

public class book_manager extends AppCompatActivity {
    private EditText nameAdd;
    private EditText authorAdd;
    private EditText publisherAdd;
    private EditText introduceAdd;
    private Button btnAddImage;
    private Button btnAddBook;
    private Button btnUploadBook;
    private ImageView showImageAdd;
    Intent intent;
    int PICK_IMAGE_REQUEST = 1; // 修改為選擇圖片的常量
    Uri uri;
    String data_list;
    StorageReference storageReference, pic_storage;

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_book_manager);
        storeSigninInfo("FCU App sign in ...");

        nameAdd = findViewById(R.id.book_name_add);
        authorAdd = findViewById(R.id.author_add);
        publisherAdd = findViewById(R.id.publisher_add);
        introduceAdd = findViewById(R.id.introduceAdd);
        btnAddBook = findViewById(R.id.book_add);
        btnAddImage = findViewById(R.id.image_add);
        showImageAdd = findViewById(R.id.image_show_add);
        btnUploadBook = findViewById(R.id.upload_image);
        storageReference = FirebaseStorage.getInstance().getReference();

        btnAddImage.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                intent = new Intent();
                intent.setFlags(Intent.FLAG_ACTIVITY_SINGLE_TOP);
                intent.setType("image/*");
                intent.setAction(Intent.ACTION_GET_CONTENT);
                startActivityForResult(intent, PICK_IMAGE_REQUEST); // 使用修改後的常量
            }
        });

        btnUploadBook.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                String bookName = nameAdd.getText().toString();
                pic_storage = storageReference.child(bookName + "." + data_list); // 添加點號分隔文件名和擴展名
                pic_storage.putFile(uri).addOnSuccessListener(new OnSuccessListener<UploadTask.TaskSnapshot>() {
                    @Override
                    public void onSuccess(UploadTask.TaskSnapshot taskSnapshot) {
                        Toast.makeText(book_manager.this, "Image Uploaded", Toast.LENGTH_SHORT).show(); // 顯示上傳成功的消息
                    }
                });
            }
        });

        View.OnClickListener listener = new View.OnClickListener() {
            @Override
            public void onClick(View view) {
                String bookName = nameAdd.getText().toString();
                String bookIntroduce = introduceAdd.getText().toString();
                String bookAuthor = authorAdd.getText().toString();
                String bookPublisher = publisherAdd.getText().toString();
                String bookState = "館藏中";
                addBook(uri.toString(), bookName, bookState, bookAuthor, bookPublisher,bookIntroduce);
                nameAdd.setText("");
                introduceAdd.setText("");
                authorAdd.setText("");
                publisherAdd.setText("");
                showImageAdd.setImageURI(null);
            }
        };
        btnAddBook.setOnClickListener(listener);
    }

    private void addBook(String uri, String bookName, String bookState, String bookAuthor, String bookPublisher,String bookIntroduce) {
        Book book = new Book(uri, bookName, bookState, bookAuthor, bookPublisher,bookIntroduce);
        FirebaseDatabase database = FirebaseDatabase.getInstance();
        DatabaseReference ref = database.getReference("books");
        ref.push().setValue(book);
        Toast.makeText(this, "Book Added", Toast.LENGTH_SHORT).show();
    }

    private void storeSigninInfo(String message) {
        FirebaseDatabase database = FirebaseDatabase.getInstance();
        DatabaseReference ref = database.getReference("Signin-Log");
        ref.setValue(new Date().getTime() + ": " + message);
    }

    @Override
    protected void onActivityResult(int requestCode, int resultCode, @Nullable Intent data) {
        if (requestCode == PICK_IMAGE_REQUEST && resultCode == RESULT_OK && data != null && data.getData() != null) { // 檢查resultCode
            uri = data.getData();
            showImageAdd.setImageURI(uri);
            ContentResolver contentResolver = getContentResolver();
            MimeTypeMap mimeTypeMap = MimeTypeMap.getSingleton();
            data_list = mimeTypeMap.getExtensionFromMimeType(contentResolver.getType(uri));
        }
        super.onActivityResult(requestCode, resultCode, data);
    }
}