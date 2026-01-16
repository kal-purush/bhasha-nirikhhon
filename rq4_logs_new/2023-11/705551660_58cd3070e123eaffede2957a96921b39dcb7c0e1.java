package com.example.taskmaster;

import static com.example.taskmaster.Addtask.TAG;

import androidx.appcompat.app.AppCompatActivity;

import android.annotation.SuppressLint;
import android.content.Intent;
import android.os.Bundle;
import android.util.Log;
import android.widget.Button;
import android.widget.EditText;

import com.amplifyframework.api.graphql.model.ModelMutation;
import com.amplifyframework.core.Amplify;
import com.amplifyframework.core.model.temporal.Temporal;
import com.amplifyframework.datastore.generated.model.Task;
import com.example.taskmaster.R;
import com.google.android.material.snackbar.Snackbar;

import java.util.Date;

public class Edit_Activity extends AppCompatActivity {
    private EditText nameEditText;
    private EditText bodyEditText;

    public Task Id ;
    public static final String TAG = "AddProductActivity";
    private EditText descriptionEditText;
    @SuppressLint("MissingInflatedId")
    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_edit);
        Intent intent = getIntent();

        nameEditText = ((EditText) findViewById(R.id.editTaskName));
        String receivedData2 = intent.getStringExtra("key");
        nameEditText.setText(receivedData2);

        bodyEditText= ((EditText) findViewById(R.id.editTaskBody));
        String receivedData3 = intent.getStringExtra("getBody");
        bodyEditText.setText(receivedData3);

       Id = Task.justId(intent.getStringExtra("Id"));
        setUpSaveButton();
        setUpDeleteButton();
    }
    private void setUpSaveButton()
    {
        Button saveButton = (Button)findViewById(R.id.saveB);

        saveButton.setOnClickListener(v ->
        {
        Task updatetask = Task.builder()

                .title(nameEditText.getText().toString())
                .id(Id.getId())
                .body(bodyEditText.getText().toString())
//                .teamTask(null)
                .build();

        System.out.println(nameEditText.getText().toString());
        Amplify.API.mutate(
                ModelMutation.update(updatetask),  // making a GraphQL request to the cloud
                successResponse ->
                {
                    Log.i(TAG, "EditProductActivity.onCreate(): edited a product successfully");
                    // TODO: Display a Snackbar
                    Snackbar.make(findViewById(R.id.Editact), "Product saved!", Snackbar.LENGTH_SHORT).show();
                },  // success callback
                failureResponse -> Log.i(TAG, "EditProductActivity.onCreate(): failed with this response: " + failureResponse)  // failure callback
        );


        });
    }


    private void setUpDeleteButton(){
        Button deleteButton = (Button) findViewById(R.id.deleteB);
        deleteButton.setOnClickListener(v ->{
            System.out.println(Id);
            Amplify.API.mutate(
                    ModelMutation.delete(Id),
                    successResponse ->
                    {
                        Log.i(TAG, "EditProductActivity.onCreate(): deleted a product successfully");
                        Intent goToProductListActivity = new Intent(Edit_Activity.this, MainActivity.class);
                        startActivity(goToProductListActivity);
                    },
                    failureResponse -> Log.i(TAG,"EditProductActivity.onCreate(): failed with this response: "+ failureResponse)
            );
        });
    }


}