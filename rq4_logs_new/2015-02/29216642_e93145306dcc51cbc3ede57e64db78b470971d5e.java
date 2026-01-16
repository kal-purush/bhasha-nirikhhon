package am.te.myapplication;

import android.support.v7.app.ActionBarActivity;
import android.os.Bundle;
import android.view.Menu;
import android.view.MenuItem;
import android.widget.ArrayAdapter;
import android.widget.ListView;

import java.util.ArrayList;
import java.util.List;


public class FriendList extends ActionBarActivity {

    // Creates the listview to hold the users.
    private ListView lv;

    @Override
    public void onCreate(Bundle saveInstanceState) {
        setContentView(R.layout.activity_friend_list);

        lv = (ListView) findViewById(R.id.friend_listView);

        // Instanciating an array list (you don't need to do this,
        // you already have yours).
        List<String> your_array_list = new ArrayList<String>();
        your_array_list.add("Dog Man I");
        your_array_list.add("Dog Man II");
        your_array_list.add("Dog Man III");
        your_array_list.add("Dog Man IV");
        your_array_list.add("Dog Man V");
        your_array_list.add("Dog Man VI");
        your_array_list.add("Dog Man VII");
        your_array_list.add("Dog Man IIX");
        your_array_list.add("Dog Man IX");
        your_array_list.add("Dog Man X");
        your_array_list.add("Dog Man XI");
        your_array_list.add("Dog Man XII");
        your_array_list.add("Dog Man XIV");


        // This is the array adapter, it takes the context of the activity as a
        // first parameter, the type of list view as a second parameter and your
        // array as a third parameter.
        ArrayAdapter<String> arrayAdapter = new ArrayAdapter<String>(
                this,
                android.R.layout.simple_list_item_1,
                your_array_list );

        lv.setAdapter(arrayAdapter);
    }


    @Override
    public boolean onCreateOptionsMenu(Menu menu) {
        // Inflate the menu; this adds items to the action bar if it is present.
        getMenuInflater().inflate(R.menu.menu_friend_list, menu);
        return true;
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