package kz.kamadi.expo.activity;

import android.content.Intent;
import android.net.Uri;
import android.support.v7.app.AppCompatActivity;
import android.os.Bundle;
import android.support.v7.widget.Toolbar;
import android.view.Menu;
import android.view.MenuItem;
import android.widget.Toast;

import butterknife.Bind;
import butterknife.ButterKnife;
import butterknife.OnClick;
import kz.kamadi.expo.R;

public class EmergencyActivity extends AppCompatActivity {

    @Bind(R.id.toolbar)
    Toolbar mToolbar;

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_emergency);
        ButterKnife.bind(this);
        setSupportActionBar(mToolbar);
        getSupportActionBar().setDisplayHomeAsUpEnabled(true);
    }
    @Override
    public boolean onOptionsItemSelected(MenuItem menuItem) {
        if (menuItem.getItemId() == android.R.id.home) {
            finish();
        }
        return super.onOptionsItemSelected(menuItem);
    }
    @OnClick(R.id.police)
    public  void callPolice(){
        Intent intent = new Intent(Intent.ACTION_CALL, Uri.parse("tel:" + "+77172102"));
        startActivity(intent);
    }
    @OnClick(R.id.medicine)
    public  void callMedicine(){
        Intent intent = new Intent(Intent.ACTION_CALL, Uri.parse("tel:" + "+77172103"));
        startActivity(intent);
    }
    @OnClick(R.id.fire)
    public  void callFire(){
        Intent intent = new Intent(Intent.ACTION_CALL, Uri.parse("tel:" + "+7717210"));
        startActivity(intent);
    }
}