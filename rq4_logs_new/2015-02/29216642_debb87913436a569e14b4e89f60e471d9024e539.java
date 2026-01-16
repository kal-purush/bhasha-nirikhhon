package am.te.myapplication;

import android.app.Activity;
import android.content.Intent;
import android.os.Bundle;
import android.view.Menu;
import android.view.MenuInflater;
import android.view.MenuItem;
import android.app.Activity;
import android.os.Bundle;
import android.view.View;
import android.view.View.OnClickListener;
import android.widget.Button;
import android.widget.EditText;
import android.widget.LinearLayout;
import android.widget.LinearLayout.LayoutParams;
import android.widget.TextView;
import android.widget.Toast;


/**
 * The homepage class acts as a springboard to other areas of the app.
 * It displays a list of deals the user is shopping for.
 *
 * @author Mitchell Manguno
 * @version 1.0
 * @since 2015 - February - 09
 */
public class FriendDetails extends Activity {
    User currentUser;

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.friend_details);


        Bundle extras = getIntent().getExtras();
        if (extras != null) {
            currentUser = User.loggedIn.getFriend(extras.getString("username"));
        }
        // Put the username onto the details screen
        TextView usernameText = (TextView) findViewById(R.id.Username);
        usernameText.setText(currentUser.getUsername());
        // Put the email onto the details screen
        TextView emailText = (TextView) findViewById(R.id.Email);
        emailText.setText(currentUser.getEmail());
        // Put the rating onto the details screen
        TextView ratingText = (TextView) findViewById(R.id.Rating);
        ratingText.setText("Rating: "+String.valueOf(currentUser.getRating()));
        // Put the email onto the details screen
        TextView salesText = (TextView) findViewById(R.id.salesReports);
        salesText.setText("Sales Reports: "+String.valueOf(currentUser.getSalesReports()));


    }


    @Override
    public boolean onCreateOptionsMenu(Menu menu) {
        // Inflate the menu items for use in the action bar
        // see http://developer.android.com/guide/topics/ui/actionbar.html#Adding
        MenuInflater inflater = getMenuInflater();
        inflater.inflate(R.menu.menu_homepage, menu);
        return super.onCreateOptionsMenu(menu);
    }

    @Override
    public boolean onOptionsItemSelected(MenuItem item) {
        // Handle presses on the action bar
        // Opens the friends menu if the user presses the 'friends' button
        // see http://developer.android.com/guide/topics/ui/actionbar.html#Adding
        switch (item.getItemId()) {
            case R.id.friend_menu:
                openFriends();
                return true;
            default:
                return super.onOptionsItemSelected(item);
        }
    }

    public void openFriends() {
        Intent intent = new Intent(this, FriendList.class);
        startActivity(intent);
    }

    public void removeFriend(View view) {
        User.loggedIn.removeFriend(currentUser);
        finish();
    }
}