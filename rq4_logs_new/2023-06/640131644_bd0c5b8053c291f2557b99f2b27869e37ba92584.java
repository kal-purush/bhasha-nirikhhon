package sg.edu.np.mad.mad_assg;

import android.content.Intent;
import android.os.Bundle;
import android.view.View;
import android.widget.Button;
import android.widget.ImageView;
import android.widget.LinearLayout;
import android.widget.TextView;

import androidx.appcompat.app.AppCompatActivity;

public class Setttings extends AppCompatActivity {

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.settings);

        LinearLayout profile = (LinearLayout) findViewById(R.id.profile_edit);
        LinearLayout history = (LinearLayout) findViewById(R.id.view_history);

        ImageView back = (ImageView) findViewById(R.id.backbtn);
        Button logout = (Button) findViewById(R.id.logoutbtn);

        back.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                Intent intent = new Intent(Setttings.this, Userpage.class);
                startActivity(intent);
            }
        });

        profile.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                Intent intent = new Intent(Setttings.this, EditUserProfile.class);
                startActivity(intent);
            }
        });

        history.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                Intent intent = new Intent(Setttings.this, HistoryViewHolder.class);
                startActivity(intent);
            }
        });
        logout.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                Intent intent = new Intent(Setttings.this, StartPage.class);
                startActivity(intent);
                finish();
            }
        });
    }
}