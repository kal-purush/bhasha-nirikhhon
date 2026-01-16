package it.polito.mad.appcomplete;

import android.content.Context;
import android.content.SharedPreferences;
import android.support.annotation.NonNull;
import android.support.v7.app.AppCompatActivity;
import android.os.Bundle;
import android.util.Log;
import android.widget.TextView;

import com.google.firebase.database.DataSnapshot;
import com.google.firebase.database.DatabaseError;
import com.google.firebase.database.DatabaseReference;
import com.google.firebase.database.FirebaseDatabase;
import com.google.firebase.database.ValueEventListener;

public class ReportActivity extends AppCompatActivity {

    TextView TextViewTotalDistance;
    TextView TextViewIncome;
    private DatabaseReference database;
    private SharedPreferences sharedpref, preferences;

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_report);

        TextViewTotalDistance = findViewById(R.id.textViewTotalDistance);
        TextViewIncome = findViewById(R.id.TextViewIncome);

    }



    private void initializeData(){
        database = FirebaseDatabase.getInstance().getReference();
        preferences = getSharedPreferences("loginState", Context.MODE_PRIVATE);

        String Uid = preferences.getString("Uid", "");
        DatabaseReference branchDailyFood = database.child("delivery/" + Uid );

        branchDailyFood.addValueEventListener(new ValueEventListener() {
            @Override
            public void onDataChange(@NonNull DataSnapshot dataSnapshot)
            {
                float totaldistance= Float.parseFloat( dataSnapshot.child("totaldistance").getValue().toString());
                TextViewTotalDistance.setText( Float.toString( totaldistance));

                Float income=totaldistance*2;
                TextViewIncome.setText(Float.toString(income));

         /*       foodList = new ArrayList<>();

                for (DataSnapshot data :  dataSnapshot.getChildren()){
                    FoodInfo value = data.getValue(FoodInfo.class);
                    value.setFoodId(data.getKey());

                    foodList.add(restoreItem(value));
                }

                initializeCardLayout();*/
            }

            @Override
            public void onCancelled(@NonNull DatabaseError databaseError) {
                //Log.w(TAG, "onCancelled: The read failed: " + databaseError.getMessage());
            }
        });
    }

}