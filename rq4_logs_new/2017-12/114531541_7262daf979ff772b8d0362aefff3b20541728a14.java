package lightup_key.bluetooth.android.iconnexys.bluetooth_lightup_key;

import android.content.Intent;
import android.support.design.widget.Snackbar;
import android.support.v7.app.AppCompatActivity;
import android.view.Menu;
import android.view.MenuItem;
import android.view.View;
import android.widget.Toast;

public class BaseActivity extends AppCompatActivity {


    @Override
    public boolean onCreateOptionsMenu(Menu menu) {
        // Inflate the menu; this adds items to the action bar if it is present.
        getMenuInflater().inflate(R.menu.menu_view, menu);
        return true;
    }

    public void clickMenuLocation(MenuItem menuItem) {
        openLocationScreen();
    }

    public void clickMenuLights(MenuItem menuItem) {
        openLightsScreen();
    }

    public void openLocationScreen() {
        Intent intent = new Intent(this, ProximityAlertActivity.class);
        startActivity(intent);
    }

    public void openLightsScreen() {
        Intent intent = new Intent(this, MainActivity.class);
        startActivity(intent);
    }

    public void showText(String text) {
        boolean toast = false;
        if (toast) {
            Toast.makeText(this, text, Toast.LENGTH_LONG).show();
        } else {
            View container = findViewById(android.R.id.content);
            if (container != null) {
                Snackbar.make(container, text, Snackbar.LENGTH_LONG).show();
            }
        }
    }

}