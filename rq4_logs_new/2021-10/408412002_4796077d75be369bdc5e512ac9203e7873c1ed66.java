package hungnp12.demo.daubep;

import androidx.appcompat.app.AppCompatActivity;

import android.content.Intent;
import android.graphics.Color;
import android.os.Bundle;
import android.view.View;
import android.widget.Button;
import android.widget.EditText;
import android.widget.Toast;

import com.google.android.material.chip.Chip;
import com.google.android.material.chip.ChipGroup;

public class NhapCongThuc extends AppCompatActivity {
    private EditText email;
    private Button submit ;
    private ChipGroup chipGroup ;
    private Button btn , btn2 ;
    Boolean btn2Flag = false ;
    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_nhap_cong_thuc);

        btn = findViewById(R.id.button);
        btn2 = findViewById(R.id.button2);
        btn2.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View view) {

                if (btn2Flag == false){
                    btn2.setTextColor(Color.parseColor("#ef4c29"));
                    btn2Flag = true ;
                }else {
                    btn2.setTextColor(Color.parseColor("#000000"));
                    System.out.println(btn2Flag);
                    btn2Flag = false ;
                }



            }
        });
        btn.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View view) {
                btn.setTextColor(Color.parseColor("#ef4c29"));

            }
        });

        email = findViewById(R.id.editText) ;
        submit = findViewById(R.id.submitEmail) ;
        chipGroup = findViewById(R.id.chipGroup);

        submit.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View view) {
                String emailText  = email.getText().toString();
                setChips(emailText);
                Toast.makeText( NhapCongThuc.this, "Thêm thành công", Toast.LENGTH_SHORT).show();
                email.setText("");
            }
        });
    }

    public void goiymonan(View view) {
        Intent intent = new Intent(this , goiymonan.class);
        startActivity(intent);
    }
    public void clickToBack(View view){
        finish();
    }

    public void setChips(String e){
        final Chip chip  = (Chip) this.getLayoutInflater().inflate(R.layout.single_input_chip_layout , null , false);
        chip.setText(e);
        chip.setOnCloseIconClickListener(new View.OnClickListener(){

            public void onClick(View view){
                chipGroup.removeView(chip);
                Toast.makeText(NhapCongThuc.this, chip.getText().toString() + " Xóa", Toast.LENGTH_SHORT).show();


            }
        });
        chipGroup.addView(chip);
    }
}