package vn.edu.usth.outlook.activities;

import android.content.Intent;
import android.os.Bundle;
import android.view.View;
import android.widget.ImageButton;
import android.widget.TextView;
import android.widget.Toast;

import androidx.appcompat.app.AppCompatActivity;

import vn.edu.usth.outlook.R;
import vn.edu.usth.outlook.database.DatabaseHelper;

public class DetailMailSentActivity extends AppCompatActivity {

    private DatabaseHelper dbHelper;
    private int emailId;
    private String senderEmail;
    private String receiverEmail;
    private String emailSubject;
    private String emailContent;
    private String emailTimestamp;

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_detail_mail_sent);

        dbHelper = new DatabaseHelper(this);

        // Nhận thông tin email từ intent khi mở từ SentFragment
        Intent intent = getIntent();
        if (intent != null) {
            emailId = intent.getIntExtra("email_id", -1); // Lấy ID của email để dễ quản lý
            senderEmail = intent.getStringExtra("sender");
            receiverEmail = intent.getStringExtra("receiver");
            emailSubject = intent.getStringExtra("subject");
            emailContent = intent.getStringExtra("content");
            emailTimestamp = intent.getStringExtra("timestamp");
        }

        if (emailId == -1) {
            Toast.makeText(this, "Email ID missing", Toast.LENGTH_SHORT).show();
            finish();
            return;
        }

        // Ánh xạ các `TextView` trong giao diện XML
        TextView senderTextView = findViewById(R.id.D_name);
        TextView receiverTextView = findViewById(R.id.toW);
        TextView subjectTextView = findViewById(R.id.D_head_email);
        TextView contentTextView = findViewById(R.id.D_content);
        TextView timestampTextView = findViewById(R.id.D_time);

        // Cập nhật giao diện với dữ liệu email
        senderTextView.setText(senderEmail);
        receiverTextView.setText(receiverEmail);
        subjectTextView.setText(emailSubject);
        contentTextView.setText(emailContent);
        timestampTextView.setText(emailTimestamp);

        // Nút trở về
        ImageButton backButton = findViewById(R.id.back_icon);
        backButton.setOnClickListener(v -> finish());

        // Nút xóa email (có thể thêm chức năng)
        ImageButton deleteButton = findViewById(R.id.delete_button);
        deleteButton.setOnClickListener(v -> {
            dbHelper.markEmailAsDeleted(emailId); // Đánh dấu email là đã xóa
            Toast.makeText(this, "Email marked as deleted", Toast.LENGTH_SHORT).show();
            finish();
        });
    }

    @Override
    protected void onDestroy() {
        super.onDestroy();
        dbHelper.close();
    }
}