package hungnp12.demo.daubep;

import androidx.appcompat.app.AppCompatActivity;

import android.content.Intent;
import android.graphics.Paint;
import android.os.Bundle;
import android.view.View;
import android.widget.EditText;
import android.widget.TextView;
import android.widget.Toast;

public class ForgotPasswordActivity extends AppCompatActivity {
    private EditText edtCode;
    private TextView txtBackLogin;

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_forgot_password);
        edtCode = findViewById(R.id.edtDigitCode);
        txtBackLogin = findViewById(R.id.txtBackLogin);
        txtBackLogin.setPaintFlags(txtBackLogin.getPaintFlags() | Paint.UNDERLINE_TEXT_FLAG);
        txtBackLogin.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View view) {
                finish();
            }
        });
    }

    public void clickToConfirm(View view) {
        String digitCode = edtCode.getText().toString();
        if(!digitCode.isEmpty()){
            Toast.makeText(this, "Xác thực thành công!", Toast.LENGTH_SHORT).show();
            finish();
        }else{
            Toast.makeText(this, "Mã xác thực sai, vui lòng nhập lại!", Toast.LENGTH_SHORT).show();
        }
    }
}