package fr.slivo.evetracker.activities;

import android.content.Intent;
import android.net.Uri;
import android.os.Bundle;
import android.support.v7.app.AppCompatActivity;
import android.view.View;
import android.widget.Button;
import android.widget.EditText;
import android.widget.TextView;
import android.widget.Toast;

import fr.slivo.evetracker.R;
import fr.slivo.evetracker.storage.StorageManager;
import fr.slivo.evetracker.alarms.AlarmReceiver;
import fr.slivo.evetracker.entities.EveAPI;

public class MainActivity extends AppCompatActivity {

    @Override
    protected void onCreate(Bundle savedInstanceState) {

        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_main);

        // Registering alarm
        AlarmReceiver.setAlarm(this);

        // Initializing api fields
        EditText keyIdField = (EditText)findViewById(R.id.keyId);
        EditText vCodeField = (EditText)findViewById(R.id.vCode);

        EveAPI eveAPI = StorageManager.getStoredApi(this);

        keyIdField.setText(eveAPI.getKeyId(), TextView.BufferType.EDITABLE);
        vCodeField.setText(eveAPI.getvCode(), TextView.BufferType.EDITABLE);

        // Create API button
        final Button createApi = (Button) findViewById(R.id.createApi);
        createApi.setOnClickListener(new View.OnClickListener() {
            public void onClick(View v) {
                Intent intent = new Intent("android.intent.action.VIEW", Uri.parse("https://community.eveonline.com/support/api-key/CreatePredefined?accessMask=8404992"));
                intent.setFlags(Intent.FLAG_ACTIVITY_SINGLE_TOP);
                startActivity(intent);
            }
        });

        // Create API button
        final Button installApi = (Button) findViewById(R.id.installApi);
        installApi.setOnClickListener(new View.OnClickListener() {
            public void onClick(View v) {
                Intent intent = new Intent("android.intent.action.VIEW", Uri.parse("https://community.eveonline.com/support/api-key/ActivateInstallLinks?activate=true"));
                intent.setFlags(Intent.FLAG_ACTIVITY_SINGLE_TOP);
                startActivity(intent);
            }
        });

        // Save API button
        final Button saveAPI = (Button) findViewById(R.id.saveAPI);
        saveAPI.setOnClickListener(new View.OnClickListener() {
            public void onClick(View v) {
                EditText keyIdField = (EditText)findViewById(R.id.keyId);
                EditText vCodeField = (EditText)findViewById(R.id.vCode);

                String api = keyIdField.getText() + ":" + vCodeField.getText();
                StorageManager.storeAPI(MainActivity.this, api);

                Toast.makeText(getApplicationContext(),"Api stored", Toast.LENGTH_SHORT).show();
            }
        });
    }

    @Override
    protected void onNewIntent(Intent intent) {
        super.onNewIntent(intent);

        EditText keyIdField = (EditText)findViewById(R.id.keyId);
        EditText vCodeField = (EditText)findViewById(R.id.vCode);
        EveAPI api = new EveAPI();

        Uri openUri = intent.getData();
        if (openUri != null ) {
            api.setKeyId(openUri.getQueryParameter("keyID"));
            api.setvCode(openUri.getQueryParameter("vCode"));
        }

        keyIdField.setText(api.getKeyId(), TextView.BufferType.EDITABLE);
        vCodeField.setText(api.getvCode(), TextView.BufferType.EDITABLE);
    }
}


