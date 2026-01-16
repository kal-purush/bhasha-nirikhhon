/* CENG BOIZ
QuickStudy
Ruel John Cootauco - N01114847
Adam Warrington - N01110575
Raymond Dang - N01048235
*/
// Team Name: cengboiz
package cengboiz.quickstudy;

import java.util.Calendar;
import java.util.Date;

import android.content.Context;
import android.content.DialogInterface;
import android.content.Intent;
import android.net.ConnectivityManager;
import android.net.NetworkInfo;
import android.support.v7.app.AlertDialog;
import android.text.format.DateFormat;
import android.app.TimePickerDialog;
import android.os.Bundle;
import android.support.v7.app.AppCompatActivity;
import android.support.v7.widget.Toolbar;
import android.view.View;
import android.widget.Button;
import android.widget.EditText;
import android.widget.ImageButton;
import android.widget.TextView;
import android.widget.TimePicker;
import android.widget.Toast;
import com.firebase.client.Firebase;


public class CreateRecurringActivity extends AppCompatActivity implements TimePickerDialog.OnTimeSetListener {

    private Firebase rootdb;
    private Button create_button;
    private EditText name_value;
    private EditText subject_value;
    private EditText info_value;
    private TextView date_value;
    private TextView clock_value;
    private TextView clock_result;
    private TextView date_result;
    private ImageButton clock_button;
    int hour, minutes;


    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_recurringcreate);

        Toolbar toolbar = (Toolbar) findViewById(R.id.mToolbar);
        setSupportActionBar(toolbar);

        if(!isConnected(this)) buildDialog(this).show();

        Firebase.setAndroidContext(this);

        Intent incomingIntent = getIntent();
        String date = incomingIntent.getStringExtra("date");

        date_result = (TextView) findViewById(R.id.date_result);
        date_result.setText(date);

        //TimePicker
        clock_button = (ImageButton) findViewById(R.id.clock);
        clock_result = (TextView) findViewById(R.id.clock_result);
        clock_button.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {


                TimePickerDialog timePickerDialog = new TimePickerDialog(CreateRecurringActivity.this, CreateRecurringActivity.this, hour,
                        minutes, DateFormat.is24HourFormat(CreateRecurringActivity.this));

                timePickerDialog.show();

            }
        });

        //Database
        rootdb = new Firebase("https://quickstudy-8d8ce.firebaseio.com/Entry");
        create_button = (Button) findViewById(R.id.postCreate);
        name_value = (EditText) findViewById(R.id.nameCreate);
        subject_value = (EditText) findViewById(R.id.subjectCreate);
        info_value = (EditText) findViewById(R.id.infoCreate);
        clock_value = (TextView) findViewById(R.id.clock_result);
        date_value = (TextView) findViewById(R.id.date_result);

        create_button.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                String name = name_value.getText().toString();
                String subject = subject_value.getText().toString();
                String info = info_value.getText().toString();
                String clock = clock_value.getText().toString();
                String date = date_value.getText().toString();


                Firebase childRef = rootdb.child("Name");
                childRef.setValue(name);

                Firebase childRef2 = rootdb.child("Subject");
                childRef2.setValue(subject);

                Firebase childRef3 = rootdb.child("Additional Info");
                childRef3.setValue(info);

                Firebase childRef4 = rootdb.child("Date");
                childRef4.setValue(date);

                Firebase childRef5 = rootdb.child("Time Duration");
                childRef5.setValue(clock);

                Toast.makeText(getApplicationContext(),"Post submitted!",Toast.LENGTH_SHORT).show();
            }
        });
    }

    @Override
    public void onTimeSet(TimePicker view, int hourOfDay, int minute) {
        Date currentTime = Calendar.getInstance().getTime();
        String delegate = "hh:mm aaa";
        String meridiem;

        if(hourOfDay > 11)
            meridiem = "PM";
        else
            meridiem = "AM";

        clock_result.setText(DateFormat.format(delegate, currentTime) + " - " + hourOfDay + ":" + minute + " " + meridiem);
    }

    @Override
    public void onBackPressed() {
        Intent newActivity = new Intent(getApplicationContext(), MainActivity.class);
        startActivity(newActivity);
    }

    public boolean isConnected(Context context) {

        ConnectivityManager cm = (ConnectivityManager) context.getSystemService(Context.CONNECTIVITY_SERVICE);
        NetworkInfo netinfo = cm.getActiveNetworkInfo();

        if (netinfo != null && netinfo.isConnectedOrConnecting()) {
            android.net.NetworkInfo wifi = cm.getNetworkInfo(ConnectivityManager.TYPE_WIFI);
            android.net.NetworkInfo mobile = cm.getNetworkInfo(ConnectivityManager.TYPE_MOBILE);

            if((mobile != null && mobile.isConnectedOrConnecting()) || (wifi != null && wifi.isConnectedOrConnecting())) return true;
            else return false;
        } else
            return false;
    }

    public AlertDialog.Builder buildDialog(Context c) {

        AlertDialog.Builder builder = new AlertDialog.Builder(c);
        builder.setTitle(R.string.no_internet);
        builder.setMessage(R.string.internet_perm);

        builder.setPositiveButton(R.string.ok, new DialogInterface.OnClickListener() {

            @Override
            public void onClick(DialogInterface dialog, int which) {

                finish();
            }
        });

        return builder;
    }
}

