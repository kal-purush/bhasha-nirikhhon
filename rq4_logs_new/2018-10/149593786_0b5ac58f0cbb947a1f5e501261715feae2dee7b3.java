package testapp.jccolumbres.stackoverflowapi.activity;

import android.support.v7.app.AppCompatActivity;
import android.os.Bundle;
import android.widget.TextView;

import java.util.Iterator;
import java.util.Map;

import testapp.jccolumbres.stackoverflowapi.R;
import testapp.jccolumbres.stackoverflowapi.adapter.UserAdapter;
import testapp.jccolumbres.stackoverflowapi.model.TopUsers;

public class DetailActivity extends AppCompatActivity {

    TextView tvDisplayName,tvReputation,tvLocation,tvBronze,tvSilver,tvGold;
    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_detail);


        initializeViews();

    }

    public void initializeViews(){
        tvDisplayName = findViewById(R.id.tv_display_name);
        tvReputation = findViewById(R.id.tv_reputation);
        tvLocation = findViewById(R.id.tv_location);

        tvBronze = findViewById(R.id.tv_bronze);
        tvSilver = findViewById(R.id.tv_silver);
        tvGold = findViewById(R.id.tv_gold);
        displayData();
    }

    public void displayData(){
        TopUsers topUsers = getIntent().getExtras().getParcelable(UserAdapter.ITEM_KEY);

        tvDisplayName.setText(topUsers.getUsername());
        tvReputation.setText(topUsers.getReputation().toString());
        tvLocation.setText(topUsers.getLocation());

        Iterator<Map.Entry<String,Integer>> it =
                topUsers.getBadges().entrySet().iterator();

        Map.Entry<String,Integer> pair = it.next();
        tvGold.setText(pair.getValue().toString());

        pair = it.next();
        tvSilver.setText(pair.getValue().toString());

        pair = it.next();
        tvBronze.setText(pair.getValue().toString());
    }
}