package plus.life.pabb.com.lifeplus6;

import android.content.Intent;
import android.os.Bundle;
import android.support.v7.app.AppCompatActivity;
import android.view.View;
import android.widget.Button;
import android.widget.TextView;

import java.util.Random;

public class CodigoRandom extends AppCompatActivity {

    private Button boton, botonatras;
    private TextView codigo;

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.codigo_random);

        boton = (Button) findViewById(R.id.boton);
        botonatras = (Button) findViewById(R.id.botonatras);
        codigo = (TextView) findViewById(R.id.codigo);

        boton.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                codigo.setText(generateString(6));
            }
        });

        botonatras.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View view) {
                Intent I = new Intent(CodigoRandom.this, UserActivity.class);
                startActivity(I);
            }

        });

    }

    private String generateString(int lenght){
        char[] chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789".toCharArray();
        StringBuilder stringBuilder = new StringBuilder();
        Random random =new Random();
        for(int i = 0; i < lenght; i++){
            char c = chars[random.nextInt(chars.length)];
            stringBuilder.append(c);
        }
        return stringBuilder.toString();
    }


}