package edu.auburn.rfid.rfind;

import android.os.Bundle;
import android.support.v7.app.ActionBarActivity;
import android.view.Menu;
import android.view.MenuItem;
import android.widget.ArrayAdapter;
import android.widget.ImageView;
import android.widget.Spinner;
import android.widget.TextView;

import java.util.ArrayList;


public class DisplayProductActivity extends ActionBarActivity {

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_display_product);

        RfidItem item = (RfidItem) ItemClickBridge.getCurrentItem();

        ImageView product_image = (ImageView) findViewById(R.id.ProductImage);

        product_image.setImageResource(R.drawable.collegeshirt);
        product_image.setPadding(8,8,8,8);
        product_image.getLayoutParams().height = 600;
        product_image.getLayoutParams().width = 600;

        TextView Vendor_Field = (TextView) findViewById(R.id.Vendor_Field);
        Vendor_Field.setText(item.getUpcDescription().getVendor());

        TextView Style_Field = (TextView) findViewById(R.id.Style_Field);
        Style_Field.setText(item.getUpcDescription().getStyle());

        TextView Color_Field = (TextView) findViewById(R.id.Color_Field);
        Color_Field.setText(item.getUpcDescription().getColor());

        TextView Size_Field = (TextView) findViewById(R.id.Size_Field);
        Size_Field.setText(item.getUpcDescription().getSize());

        TextView Fit_Field = (TextView) findViewById(R.id.Fit_Field);
        Fit_Field.setText(item.getUpcDescription().getFit());

        Spinner Location_List = (Spinner) findViewById(R.id.Location_List);
        ArrayAdapter<CharSequence> adapter = new ArrayAdapter<>(this, android.R.layout.simple_spinner_item, getLocations(item));
        adapter.setDropDownViewResource(android.R.layout.simple_spinner_dropdown_item);
        Location_List.setAdapter(adapter);
    }

    private ArrayList<CharSequence> getLocations(RfidItem Item) {
        ArrayList<CharSequence> locations = new ArrayList<>();

        for (Product product : Item.getProducts()) {
            locations.add(product.getLocation());
        }

        return locations;
    }

    @Override
    public boolean onCreateOptionsMenu(Menu menu) {
        // Inflate the menu; this adds items to the action bar if it is present.
        getMenuInflater().inflate(R.menu.menu_display_product, menu);
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