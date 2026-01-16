package boilerride.com.boilerride;

import android.os.Bundle;
import android.support.design.widget.FloatingActionButton;
import android.support.design.widget.Snackbar;
import android.support.v7.app.AppCompatActivity;
import android.support.v7.widget.Toolbar;
import android.view.View;
import android.widget.Button;
import android.widget.EditText;
import android.widget.RatingBar;
import android.widget.TextView;
import android.widget.Toast;

import com.firebase.client.Firebase;

import java.util.HashMap;
import java.util.Map;

public class RateIndividualPassengerActivity extends AppCompatActivity {

    private TextView txtRatingValue;
    private RatingBar ratingBar;
    private EditText tv_textview;
    private Button btnSubmit;
    private double score;
    private Firebase myFirebase = new Firebase("https://luminous-torch-1510.firebaseio.com/ratings");

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_rate_driver);
        tv_textview = (EditText)findViewById(R.id.rate_driver123);
        addListenerOnRatingBar();
        addListenerOnButton();
    }
    public void addListenerOnRatingBar() {

        ratingBar = (RatingBar) findViewById(R.id.ratingBar);
        txtRatingValue = (TextView) findViewById(R.id.txtRatingValue);

        //if rating value is changed,
        //display the current rating value in the result (textview) automatically
        ratingBar.setOnRatingBarChangeListener(new RatingBar.OnRatingBarChangeListener() {
            public void onRatingChanged(RatingBar ratingBar, float rating,
                                        boolean fromUser) {
                score = rating;
                txtRatingValue.setText(String.valueOf(rating));

            }
        });
    }

    public void addListenerOnButton() {

        ratingBar = (RatingBar) findViewById(R.id.ratingBar);
        btnSubmit = (Button) findViewById(R.id.btnSubmit);

        //if click on me, then display the current rating value.
        btnSubmit.setOnClickListener(new View.OnClickListener() {

            @Override
            public void onClick(View v) {
                Firebase userComments = myFirebase.child(CentralData.passengerKey);
                String comment = tv_textview.getText().toString();
                Map<String, String> post1 = new HashMap<String, String>();
                post1.put("reviewer", CentralData.uid);
                post1.put("reviewed", CentralData.rideCreatorUid);
                post1.put("rate", String.valueOf(score));
                post1.put("comment", comment);
                userComments.push().setValue(post1);

                Toast.makeText(RateIndividualPassengerActivity.this, String.valueOf(ratingBar.getRating()), Toast.LENGTH_SHORT).show();

            }

        });
    }
}