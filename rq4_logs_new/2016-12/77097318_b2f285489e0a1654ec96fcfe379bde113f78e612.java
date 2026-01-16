package nyc.c4q.akashaarcher.animalhaus;

import android.os.Bundle;
import android.support.v7.app.AppCompatActivity;
import android.view.View;
import android.widget.Button;
import android.widget.TextView;

/**
 * Created by akashaarcher on 12/21/16.
 */

public class ExtraCreditActivity extends AppCompatActivity {

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.extra_credit_activity);

        final TextView tvStateOne = (TextView) findViewById(R.id.state_one);
        final TextView tvStateTwo = (TextView) findViewById(R.id.state_two);


        Button changeBtn = (Button) findViewById(R.id.state_btn);
        changeBtn.setOnClickListener(
                new View.OnClickListener() {
                    @Override
                    public void onClick(View v) {
                        if (tvStateOne.getVisibility() == tvStateOne.VISIBLE) {
                            tvStateTwo.setVisibility(View.VISIBLE);
                            tvStateOne.setVisibility(View.INVISIBLE);
                        } else if (tvStateTwo.getVisibility() == tvStateTwo.VISIBLE) {
                            tvStateTwo.setVisibility(View.INVISIBLE);
                            tvStateOne.setVisibility(View.VISIBLE);
                        }
                    }
                });
    }

}