package com.example.its_a_feature_not_a_bug;

import android.app.AlertDialog;
import android.app.Dialog;
import android.content.Context;
import android.content.Intent;
import android.net.Uri;
import android.view.LayoutInflater;
import android.view.View;
import android.os.Bundle;
import android.widget.Button;
import android.widget.DatePicker;
import android.widget.EditText;
import android.widget.ImageView;

import androidx.activity.result.ActivityResultLauncher;
import androidx.activity.result.contract.ActivityResultContracts;
import androidx.annotation.NonNull;
import androidx.annotation.Nullable;
import androidx.fragment.app.DialogFragment;

import java.net.URI;
import java.util.Calendar;
import java.util.Date;


public class AddEventFragment extends DialogFragment {
    private AddEventDialogueListener listener;
    private static final int IMAGE_PICK_REQUEST = 1;
    private ActivityResultLauncher<String> imagePickerLauncher;
    private Uri selectedImageUri;

    public AddEventFragment() {}

    @Override
    public void onAttach(@NonNull Context context) {
        super.onAttach(context);
        if (context instanceof AddEventDialogueListener) {
            listener = (AddEventDialogueListener) context;
        } else {
            throw new RuntimeException(context + "must implement AddEventDialogueListener");
        }
    }

    @NonNull
    @Override
    public Dialog onCreateDialog(@Nullable Bundle savedInstanceState) {
        View view = LayoutInflater.from(getContext()).inflate(R.layout.fragment_add_event, null);

        EditText editEventId = view.findViewById(R.id.edit_text_event_title);
        EditText editEventHost = view.findViewById(R.id.edit_tex_event_host);
        DatePicker editEventDate = view.findViewById(R.id.date_picker_event_date);
        EditText editEventDescription = view.findViewById(R.id.edit_text_event_description);
        ImageView eventPoster = view.findViewById(R.id.image_view_event_image);
        Button uploadButton = view.findViewById(R.id.upload_button);

        uploadButton.setOnClickListener(v -> {
            imagePickerLauncher.launch("image/*");
        });

        imagePickerLauncher = registerForActivityResult(new ActivityResultContracts.GetContent(),
                result -> {
                    if (result != null) {
                        // Set the selected image URI
                        selectedImageUri = result;
                        // Set the selected image to ImageView
                        eventPoster.setImageURI(result);
                    }
                });

        AlertDialog.Builder builder = new AlertDialog.Builder(getContext());
        return builder
                .setView(view)
                .setTitle("Add Event")
                .setNegativeButton("cancel", null)
                .setPositiveButton("ok", (dialog, which) -> {
                    String title = editEventId.getText().toString();
                    String host = editEventHost.getText().toString();
                    int year = editEventDate.getYear();
                    int month = editEventDate.getMonth();
                    int dayOfMonth = editEventDate.getDayOfMonth();
                    Calendar calendar = Calendar.getInstance();
                    calendar.set(year, month, dayOfMonth);
                    Date date = calendar.getTime();
                    String description = editEventDescription.getText().toString();

                    listener.addEvent(new Event(selectedImageUri, title, host, date, description));
                })
                .create();

    }

//    public void addEvent(Bundle savedInstanceState) {
//        super.onCreate(savedInstanceState);
//
//        setContentView(R.layout.activity_add_event);
//        eventName = findViewById(R.id.eventName);
//        eventDate = findViewById(R.id.eventDate);
//        eventLocation = findViewById(R.id.eventLocation);
//        eventDescription = findViewById(R.id.eventDescription);
//        eventTime = findViewById(R.id.eventTime);
//        createButton = findViewById(R.id.createButton);
//        date.setOnClickListener(new View.OnClickListener() {
//            @Override
//            public void onClick(View v) {
//                showDatePickerDialog();
//            }
//        });
//        time.setOnClickListener(new View.OnClickListener() {
//            @Override
//            public void onClick(View v) {
//                showTimePickerDialog();
//            }
//        });
//        createButton.setOnClickListener(new View.OnClickListener() {
//            @Override
//            public void onClick(View v) {
//                createEvent();
//            }
//        });
//        public void onCreate(Bundle savedInstanceState) {
//            View view = inflater.inflate(R.layout.fragment_add_event, container, false);
//
//
//
//        }
//    }
}