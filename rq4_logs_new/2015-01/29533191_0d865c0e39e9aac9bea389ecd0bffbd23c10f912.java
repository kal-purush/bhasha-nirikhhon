package asyrkett.android.jokeofthedayapp;

import java.util.Random;

import android.app.Activity;
import android.os.Bundle;
import android.view.Menu;
import android.view.MenuItem;
import android.widget.TextView;


public class MainActivity extends Activity {
	
	private static Random GENERATOR = new Random();
	
	//Jokes from http://www.ducksters.com/jokesforkids/computer.php
	private static String[] JOKE_LIST = new String[]{
		"What did the spider do on the computer?\nMake a website!",
		"What did the computer do at lunchtime?\nHave a byte to eat!",
		"Why did the computer keep sneezing?\nIt had a virus!",
		"What is a computer virus?\nA terminal illness!",
		"Why was the computer cold?\nIt left its Windows open!",
		"Why did the computer squeak?\nBecause someone stepped on its mouse!",
		"What do you get when you cross a computer and a life guard?\nA screensaver!",
		"Where do all the cool mice live?\nIn thier mousepads!"
	};
	
    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_main);
        
        TextView jokeTextView = (TextView) findViewById(R.id.joke_text_view);
        jokeTextView.setText(JOKE_LIST[GENERATOR.nextInt(JOKE_LIST.length)]);
    }


    @Override
    public boolean onCreateOptionsMenu(Menu menu) {
        // Inflate the menu; this adds items to the action bar if it is present.
        getMenuInflater().inflate(R.menu.main, menu);
        return true;
    }

    @Override
    public boolean onOptionsItemSelected(MenuItem item) {
        // Handle action bar item clicks here. The action bar will
        // automatically handle clicks on the Home/Up button, so long
        // as you specify a parent activity in AndroidManifest.xml.
        int id = item.getItemId();
        if (id == R.id.action_settings) {
            return true;
        }
        return super.onOptionsItemSelected(item);
    }
}