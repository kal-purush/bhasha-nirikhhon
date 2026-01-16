package edu.upc.eetac.dsa.dsaqt1415g4.utroll;

import android.app.Activity;
import android.app.ProgressDialog;
import android.content.Context;
import android.os.AsyncTask;
import android.os.Bundle;
import android.view.Gravity;
import android.view.View;
import android.widget.EditText;
import android.widget.Toast;

import edu.upc.eetac.dsa.dsaqt1415g4.utroll.api.AppException;
import edu.upc.eetac.dsa.dsaqt1415g4.utroll.api.User;
import edu.upc.eetac.dsa.dsaqt1415g4.utroll.api.uTrollAPI;

public class UpdateUserActivity extends Activity {
    private final static String TAG = UpdateUserActivity.class.getName();

    private class updateUserTask extends AsyncTask<String, Void, User> {
        private ProgressDialog pd;

        @Override
        protected User doInBackground(String... params) {
            User user = new User();
            try {
                int age;
                String name;
                String email;
                if (params[1].equals(""))
                    age = 0;
                else
                    age = Integer.parseInt(params[1]);

                if (params[0].equals(""))
                    name = null;
                else
                    name = params[0];

                if (params[2].equals(""))
                    email = null;
                else
                    email = params[2];

                user = uTrollAPI.getInstance(UpdateUserActivity.this).updateUser(name, age, email);
            } catch (AppException e) {
                e.printStackTrace();
            }
            return user;
        }

        @Override
        protected void onPostExecute(User result) {
            finish();
            if (pd != null) {
                pd.dismiss();
            }
        }

        @Override
        protected void onPreExecute() {
            pd = new ProgressDialog(UpdateUserActivity.this);
            pd.setTitle("Searching...");
            pd.setCancelable(false);
            pd.setIndeterminate(true);
            pd.show();
        }

    }

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.update_user_layout);
    }

    public void updateUser(View v) {
        EditText etName = (EditText) findViewById(R.id.etUpdateUserName);
        EditText etAge = (EditText) findViewById(R.id.etUpdateUserAge);
        EditText etEmail = (EditText) findViewById(R.id.etUpdateUserEmail);

        String name = etName.getText().toString();
        String ageS = etAge.getText().toString();
        String email = etEmail.getText().toString();

        (new updateUserTask()).execute(name, ageS, email);
    }
}