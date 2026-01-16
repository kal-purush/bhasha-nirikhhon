package mobile.project.cekjadwaldokter.Akun;

import android.app.ProgressDialog;
import android.content.Intent;
import android.os.Bundle;
import android.support.annotation.NonNull;
import android.support.v7.app.AppCompatActivity;
import android.text.TextUtils;
import android.view.View;
import android.widget.Button;
import android.widget.EditText;
import android.widget.TextView;
import android.widget.Toast;

import com.google.android.gms.tasks.OnCompleteListener;
import com.google.android.gms.tasks.Task;
import com.google.firebase.auth.AuthResult;
import com.google.firebase.auth.FirebaseAuth;
import com.google.firebase.auth.FirebaseUser;
import com.google.firebase.database.DatabaseReference;
import com.google.firebase.database.FirebaseDatabase;

import java.util.Date;

import mobile.project.cekjadwaldokter.R;

public class DaftarActivity extends AppCompatActivity implements View.OnClickListener {

    EditText name,email,password, noTlp;
    Button mRegisterbtn;
    TextView mLoginPageBack;
    FirebaseAuth mAuth;
    DatabaseReference mdatabase;
    String Name,Email,Password, NoTelp;
    ProgressDialog mDialog;

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_daftar);
        name = (EditText)findViewById(R.id.id_nama);
        noTlp = (EditText) findViewById(R.id.idnoHP);
        email = (EditText)findViewById(R.id.id_email);
        password = (EditText)findViewById(R.id.id_password);
        mRegisterbtn = (Button)findViewById(R.id.buttonRegister);

        // for authentication using FirebaseAuth.
        mAuth = FirebaseAuth.getInstance();
        mRegisterbtn.setOnClickListener(this);
        mDialog = new ProgressDialog(this);
        mdatabase = FirebaseDatabase.getInstance().getReference().child("Users");
    }

    public void onClick(View v) {
        if (v==mRegisterbtn){
            UserRegister();
        }
    }

    private void UserRegister() {
        Name = name.getText().toString().trim();
        NoTelp = noTlp.getText().toString().trim();
        Email = email.getText().toString().trim();
        Password = password.getText().toString().trim();

        if (TextUtils.isEmpty(Name)){
            Toast.makeText(DaftarActivity.this, "Masukkan Nama", Toast.LENGTH_SHORT).show();
            return;
        }else if (TextUtils.isEmpty(Email)){
            Toast.makeText(DaftarActivity.this, "Masukkan Email", Toast.LENGTH_SHORT).show();
            return;
        }else if (TextUtils.isEmpty(NoTelp)){
            Toast.makeText(DaftarActivity.this, "Masukkan No Telp", Toast.LENGTH_SHORT).show();
            return;
        }else if (TextUtils.isEmpty(Password)){
            Toast.makeText(DaftarActivity.this, "Masukkan Password", Toast.LENGTH_SHORT).show();
            return;
        }else if (Password.length()<6){
            Toast.makeText(DaftarActivity.this,"Masukkan Password Minimal 6 Karakter",Toast.LENGTH_SHORT).show();
            return;
        }
        mDialog.setMessage("Creating User please wait...");
        mDialog.setCanceledOnTouchOutside(false);
        mDialog.show();
        mAuth.createUserWithEmailAndPassword(Email,Password).addOnCompleteListener(new OnCompleteListener<AuthResult>() {
            @Override
            public void onComplete(@NonNull Task<AuthResult> task) {
                if (task.isSuccessful()){
                    sendEmailVerification();
                    mDialog.dismiss();
                    OnAuth(task.getResult().getUser());
                    mAuth.signOut();
                }else if (!task.isSuccessful()){
                    Toast.makeText(DaftarActivity.this,"Format Email Salah",Toast.LENGTH_SHORT).show();
                    mDialog.dismiss();
                }
                else{
                    Toast.makeText(DaftarActivity.this,"Email Atau Password Sudah Ada",Toast.LENGTH_SHORT).show();
                    mDialog.dismiss();
                }
            }
        });
    }
    //Email verification code using FirebaseUser object and using isSucccessful()function.
    private void sendEmailVerification() {
        FirebaseUser user = FirebaseAuth.getInstance().getCurrentUser();
        if (user!=null){
            user.sendEmailVerification().addOnCompleteListener(new OnCompleteListener<Void>() {
                @Override
                public void onComplete(@NonNull Task<Void> task) {
                    if (task.isSuccessful()){
                        Toast.makeText(DaftarActivity.this,"Sukses, Cek Email Anda Untuk Verifikasi",Toast.LENGTH_SHORT).show();
                        FirebaseAuth.getInstance().signOut();
                        Intent intent = new Intent(DaftarActivity.this, LoginActivity.class);
                        startActivity(intent);
                        finish();
                    }
                }
            });
        }
    }

    private void OnAuth(FirebaseUser user) {
        createAnewUser(user.getUid());
    }

    private void createAnewUser(String uid) {
        User user = BuildNewuser();
        mdatabase.child(uid).setValue(user);
    }


    private User BuildNewuser(){
        return new User(
                getDisplayName(),
                getNoTelp(),
                getUserEmail(),
                getPassword(),
                new Date().getTime()
        );
    }

    public String getDisplayName() {

        return Name;
    }

    public  String getNoTelp(){

        return NoTelp;
    }

    public String getUserEmail() {

        return Email;
    }

    public String getPassword(){

        return Password;
    }
}