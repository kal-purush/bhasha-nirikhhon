package am.te.myapplication;

import android.app.Activity;

import android.os.Bundle;
import android.util.Log;
import android.view.Menu;
import android.view.MenuItem;
import android.view.View;
import android.widget.AdapterView;
import android.widget.ArrayAdapter;
import android.widget.EditText;
import android.widget.ListView;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.HashSet;


public class SearchFriends extends Activity {

    private static Set<User> registeredUsers;
    private EditText queryView;
    private List<User> matches;
    private ListView lv;
    private ArrayAdapter<User> arrayAdapter;

    @Override
    public void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_search_friends);
        lv = (ListView) findViewById(R.id.search_friend_listview);
        queryView = (EditText) findViewById(R.id.search_editText);
        matches = new ArrayList<User>();
        registeredUsers = populateUsers();

        /* Get the intent, verify the action and get the query
        Intent intent = getIntent();
        if (Intent.ACTION_SEARCH.equals(intent.getAction())) {
            String query = intent.getStringExtra(SearchManager.QUERY);
            search(query);
        }*/
        arrayAdapter = new ArrayAdapter<User>(this, android.R.layout.simple_list_item_1, matches);

        lv.setAdapter(arrayAdapter);
        lv.setOnItemClickListener(new AdapterView.OnItemClickListener() {
            @Override
            public void onItemClick(AdapterView<?> parent, View view, int position, long id) {
                //add friend to friend list here
                Log.d(AddFriend.class.getSimpleName(), "position" + position + " id" + id);
                //local
                if (State.local) {
                    User.loggedIn.addFriend(matches.get(position));
                    matches.remove(position);
                    arrayAdapter.notifyDataSetChanged();
                } else {

                }
            }
        });

        super.onStart();
    }


    /**
     * The OnClick listener for the "OK" button. Calls the real search method
     * with whatever is inside the textfield at the time.
     *
     * @param view Not really sure what this is for
     */
    public void search(View view) {
        matches = new ArrayList<User>();
        String query = queryView.getText().toString();
        search(query);
    }


    @Override
    public boolean onCreateOptionsMenu(Menu menu) {
        // Inflate the menu; this adds items to the action bar if it is present.
        getMenuInflater().inflate(R.menu.menu_search_friends, menu);
        return true;
    }

    /**
     * Searches through the registered users. Currently, it is a set of Users.
     *
     * @param query the String query that we will search with
     */
    private void search(String query) {
        matches = new ArrayList<User>();
        Iterator<User> iter = registeredUsers.iterator();
        User curr = null;
        boolean email = query.contains("@");
        while (iter.hasNext()) {
            curr = iter.next();
            System.out.println("User is:" + curr.getUsername());
            if (email) {
                if (curr != null && query.equals(curr.getEmail())) {
                    System.out.println("Found a match");
                    matches.add(curr);
                }
            } else {
                if (curr != null && query.equals(curr.getUsername())) {
                    System.out.println("Found a match");
                    matches.add(curr);
                }
            }
        }

        arrayAdapter.clear();
        arrayAdapter.addAll(matches);
        arrayAdapter.notifyDataSetChanged();

    }

    /**
     * Polls whatever powers that be to populate the Registered Users set
     *
     * @return a set of all registered users who are not your friends
     */
    private Set populateUsers() {
        Set<User> users = new HashSet<User>();
        if (State.local) {
            ArrayList<User> toAdd = new ArrayList<>();
            toAdd.add(new User("Dog Man L", "woofwoof", "dog@man.com"));
            toAdd.add(new User("frog", "qwrg", "frog@leg.biz"));
            toAdd.add(new User("toad", "xfsdf", "collin@126.xxx"));
            toAdd.add(new User("cricket", "asrgh", "ypres@wat.ru"));

            for (User possibleFriend: toAdd) {
                if (!User.loggedIn.isFriendsWith(possibleFriend)) {

                    users.add(possibleFriend);
                }
            }
        } else {
            //database integration goes here
        }

        return users;
    }

    @Override
    public boolean onOptionsItemSelected(MenuItem item) {
        // Handle action bar item clicks here. The action bar will
        // automatically handle clicks on the Home/Up button, so long
        // as you specify a parent activity in AndroidManifest.xml.
        int id = item.getItemId();

        //noinspection SimplifiableIfStatement
        if (id == R.id.action_settings) {
            return true;
        }

        return super.onOptionsItemSelected(item);
    }
}