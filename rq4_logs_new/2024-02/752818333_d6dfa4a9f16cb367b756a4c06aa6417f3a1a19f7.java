package com.android.quemeful_qr;

import androidx.activity.result.ActivityResultLauncher;
import androidx.activity.result.PickVisualMediaRequest;
import androidx.activity.result.contract.ActivityResultContracts;
import androidx.appcompat.app.AppCompatActivity;

import android.app.DatePickerDialog;
import android.app.TimePickerDialog;
import android.content.Intent;
import android.os.Bundle;
import android.view.View;
import android.widget.Button;
import android.widget.DatePicker;
import android.widget.ImageButton;
import android.widget.TimePicker;
import android.widget.Toast;

import com.google.android.material.textfield.TextInputEditText;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Locale;

/**
 * Draft copy, code to be reviewed again.
 * This activity creates and new event.
 * Yet to do: 1. add firebase to store the new event.
 *            2. add the "Use Existing QR Code" part (see mockup create event page).
 *            3. write test cases
 *           (4) needs to handle empty field cases by displaying a toast "please fill all fields" before creating event.
 * Reference URLs (yet to specify author, license,... will add them later. This is just a record.)
 * https://www.youtube.com/watch?v=cxEb4IafzZU
 * https://developer.android.com/training/data-storage/shared/photopicker
 */
public class CreateNewEventActivity extends AppCompatActivity {
    // declare variables
    private TextInputEditText eventTitle;
    private TextInputEditText eventDescription;
    private TextInputEditText startDate;
    private TextInputEditText startTime;
    private TextInputEditText endDate;
    private TextInputEditText endTime;
    private Button generateQRButton;
    private Button cancelButton;
    private Button createButton;
    private ImageButton uploadPoster;

    //for date and time picker
    Calendar calendar = Calendar.getInstance();
    Calendar time = Calendar.getInstance();

//    // get the firestore db instance
//    FirebaseFirestore db = FirebaseFirestore.getInstance();

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_create_event);

        // initialize variables
        eventTitle = findViewById(R.id.enter_title);
        eventDescription = findViewById(R.id.enter_event_details);
        startDate = findViewById(R.id.enter_startDate);
        startTime = findViewById(R.id.enter_startTime);
        endDate = findViewById(R.id.enter_endDate);
        endTime = findViewById(R.id.enter_endTime);
        generateQRButton = findViewById(R.id.QR_generate_button_for_createEvent);
        cancelButton = findViewById(R.id.cancel_button);
        createButton = findViewById(R.id.create_button);
        uploadPoster = findViewById(R.id.add_poster_button);

        // for adding poster by user added READ_MEDIA_IMAGES permission in AndroidManifest.xml
        // start image selecting activity and get its result using ActivityResultLauncher
        ActivityResultLauncher<PickVisualMediaRequest> pickImage = registerForActivityResult(
                new ActivityResultContracts.PickVisualMedia(),
                uri -> {
                    if (uri != null) {
                        // display the selected image
                        uploadPoster.setImageURI(uri);
                    }
                }
        );

        // initiate selection of image when the add button is clicked using Intent
        // add poster
        uploadPoster.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                // allow user to select image from gallery
                pickImage.launch(new PickVisualMediaRequest.Builder()
                        .setMediaType(ActivityResultContracts.PickVisualMedia.ImageOnly.INSTANCE)
                        .build());
            }
        });

        // set listener on create button
        createButton.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                // retrieve user input for event title and details
                String title = eventTitle.getText().toString();
                String description = eventDescription.getText().toString();

//                // Create a new event object in db
//                Map<String, Object> NewEvent = new HashMap<>();
//                NewEvent.put("Event Title", title);
//                NewEvent.put("Event Details", description);

                // set the retrieved details in their respective text boxes
                eventTitle.setText(title);
                eventDescription.setText(description);
                // call selectDate and selectTime
                selectDate();
                selectTime();
                Toast.makeText(CreateNewEventActivity.this, "New Event successfully created", Toast.LENGTH_SHORT).show();
            }
        });

        generateQRButton.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                Intent intent = new Intent(CreateNewEventActivity.this, GenerateNewQRActivity.class);
                startActivity(intent);
            }
        });

        cancelButton.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                Toast.makeText(CreateNewEventActivity.this, "Create New Event Cancelled", Toast.LENGTH_SHORT).show();
                finish();
            }
        });
    }

    /**
     * This method is to get the start and the end date from user.
     * selectDate() allows user to pick date.
     * setDateFormat() is a method used to set the format for displaying the date in dd/MM/yyyy.
     */
    private void selectDate(){
        DatePickerDialog.OnDateSetListener date = new DatePickerDialog.OnDateSetListener() {
            @Override
            public void onDateSet(DatePicker view, int year, int month, int dayOfMonth) {
                calendar.set(Calendar.YEAR, year);
                calendar.set(Calendar.MONTH, month);
                calendar.set(Calendar.DAY_OF_MONTH, dayOfMonth);

                // set the date in its fields
                startDate.setText(setDateFormat());
                endDate.setText(setDateFormat());
            }
        };

        // start date picker and end date picker works the same way
        startDate.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                new DatePickerDialog(CreateNewEventActivity.this, date, calendar.get(Calendar.YEAR), calendar.get(Calendar.MONTH), calendar.get(Calendar.DAY_OF_MONTH)).show();
            }
        });

        endDate.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                new DatePickerDialog(CreateNewEventActivity.this, date, calendar.get(Calendar.YEAR), calendar.get(Calendar.MONTH), calendar.get(Calendar.DAY_OF_MONTH)).show();
            }
        });
    }

    private String setDateFormat(){
        String format = "dd/MM/yyyy";
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat(format, Locale.CANADA);
        return simpleDateFormat.format(calendar.getTime());
    }

    /**
     * This method is to get the start and the end time from user.
     * selectTime() allows user to pick time.
     * setTimeFormat() is a method used to set the format for displaying the time in HH:mm.
     */
    private void selectTime(){
        int hours = time.get(Calendar.HOUR_OF_DAY);
        int minutes = time.get(Calendar.MINUTE);
        TimePickerDialog.OnTimeSetListener timePickerDialog = new TimePickerDialog.OnTimeSetListener() {
            @Override
            public void onTimeSet(TimePicker view, int hourOfDay, int minute) {
                time.set(Calendar.HOUR_OF_DAY, hours);
                time.set(Calendar.MINUTE, minutes);

                // set the times in their fields
                startTime.setText(setTimeFormat());
                endTime.setText(setTimeFormat());
            }
        };

        startTime.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                new TimePickerDialog(CreateNewEventActivity.this, (TimePickerDialog.OnTimeSetListener) time, hours, minutes, true).show();
            }
        });
        endTime.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                new TimePickerDialog(CreateNewEventActivity.this, (TimePickerDialog.OnTimeSetListener) time, hours, minutes, true).show();
            }
        });
    }
    private String setTimeFormat(){
        String format = "HH:mm";
        SimpleDateFormat timeFormat = new SimpleDateFormat(format, Locale.CANADA);
        return timeFormat.format(time.getTime());
    }
}