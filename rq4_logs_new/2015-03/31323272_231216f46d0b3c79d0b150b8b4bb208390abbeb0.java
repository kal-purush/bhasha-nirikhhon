package jjohnson.yeoman01;

import android.support.v7.app.ActionBarActivity;
import android.os.Bundle;
import android.view.Menu;
import android.view.MenuItem;
import android.view.KeyEvent;
import android.view.View;
import android.view.View.OnKeyListener;
import android.view.inputmethod.EditorInfo;
import android.widget.EditText;
import android.widget.TextView;


public class createEntity extends ActionBarActivity implements OnKeyListener{

    EditText[] textFields = new EditText[18];

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_create_entity);

        EditText[] textFields = new EditText[18];

        textFields[0] = (EditText) findViewById(R.id.name);
        textFields[0].setOnKeyListener(this);
        textFields[1] = (EditText) findViewById(R.id.level);
        textFields[1].setOnKeyListener(this);
        textFields[2] = (EditText) findViewById(R.id.STR);
        textFields[2].setOnKeyListener(this);
        textFields[3] = (EditText) findViewById(R.id.DEX);
        textFields[3].setOnKeyListener(this);
        textFields[4] = (EditText) findViewById(R.id.CON);
        textFields[4].setOnKeyListener(this);
        textFields[5] = (EditText) findViewById(R.id.INT);
        textFields[5].setOnKeyListener(this);
        textFields[6] = (EditText) findViewById(R.id.WIS);
        textFields[6].setOnKeyListener(this);
        textFields[7] = (EditText) findViewById(R.id.CHA);
        textFields[7].setOnKeyListener(this);
        textFields[8] = (EditText) findViewById(R.id.HP);
        textFields[8].setOnKeyListener(this);
        textFields[9] = (EditText) findViewById(R.id.init);
        textFields[9].setOnKeyListener(this);
        textFields[10] = (EditText) findViewById(R.id.speed);
        textFields[10].setOnKeyListener(this);
        textFields[11] = (EditText) findViewById(R.id.fort);
        textFields[11].setOnKeyListener(this);
        textFields[12] = (EditText) findViewById(R.id.reflex);
        textFields[12].setOnKeyListener(this);
        textFields[13] = (EditText) findViewById(R.id.will);
        textFields[13].setOnKeyListener(this);
        textFields[14] = (EditText) findViewById(R.id.AC);
        textFields[14].setOnKeyListener(this);
        textFields[15] = (EditText) findViewById(R.id.flatfoot);
        textFields[15].setOnKeyListener(this);
        textFields[16] = (EditText) findViewById(R.id.touch);
        textFields[16].setOnKeyListener(this);
        textFields[17] = (EditText) findViewById(R.id.baseattack);
        textFields[17].setOnKeyListener(this);



    }


    @Override
    public boolean onCreateOptionsMenu(Menu menu) {
        // Inflate the menu; this adds items to the action bar if it is present.
        getMenuInflater().inflate(R.menu.menu_create_entity, menu);
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


    @Override
    public boolean onKey(View view, int keyCode, KeyEvent event) {

        EditText myEditText = (EditText) view;

        if (keyCode == EditorInfo.IME_ACTION_SEARCH ||
                keyCode == EditorInfo.IME_ACTION_DONE ||
                event.getAction() == KeyEvent.ACTION_DOWN &&
                        event.getKeyCode() == KeyEvent.KEYCODE_ENTER) {

            if (!event.isShiftPressed()) {
                int i;
                int id = view.getId();
                for (i = 0; i < 18; ++i) {
                    if (id == textFields[i].getId())
                        break;
                }
                if ((i+1) > 17)
                    i = -1;
                textFields[i + 1].requestFocus();
                return true;
            }

        }
        return false; // pass on to other listeners.

    }
}