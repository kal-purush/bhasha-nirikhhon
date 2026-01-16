package com.curdrome.agenziaispjdm.adapters;

import android.content.Context;
import android.view.LayoutInflater;
import android.view.View;
import android.view.ViewGroup;
import android.widget.ArrayAdapter;
import android.widget.ImageButton;
import android.widget.TextView;

import com.curdrome.agenziaispjdm.R;
import com.curdrome.agenziaispjdm.main.MainActivity;
import com.curdrome.agenziaispjdm.model.Property;

import org.json.JSONObject;

import java.util.List;

/**
 * Created by adria on 21/09/2015.
 */
public class BookmarkAdapter extends ArrayAdapter<Property> {

    MainActivity activity;

    public BookmarkAdapter(Context context, int resource, List<Property> objects, MainActivity mainActivity) {
        super(context, resource, objects);
        activity = mainActivity;
    }

    @Override
    public View getView(int position, View convertView, ViewGroup parent) {


        LayoutInflater inflater = (LayoutInflater) getContext()
                .getSystemService(Context.LAYOUT_INFLATER_SERVICE);
        convertView = inflater.inflate(R.layout.bookmark_row, null);

        TextView province = (TextView) convertView.findViewById(R.id.Provincia);
        TextView city = (TextView) convertView.findViewById(R.id.Citta);
        TextView zone = (TextView) convertView.findViewById(R.id.Zona);
        TextView rooms = (TextView) convertView.findViewById(R.id.NumeroStanze);
        TextView bath = (TextView) convertView.findViewById(R.id.NumeroBagni);
        TextView type = (TextView) convertView.findViewById(R.id.Tipo);
        TextView subtype = (TextView) convertView.findViewById(R.id.SottoTipo);
        TextView price = (TextView) convertView.findViewById(R.id.Prezzo);
        TextView sqm = (TextView) convertView.findViewById(R.id.Mq);
        TextView description = (TextView) convertView.findViewById(R.id.Descrizione);
        TextView foto_link = (TextView) convertView.findViewById(R.id.Foto);

        ImageButton removeBookmark = (ImageButton) convertView.findViewById(R.id.removeBookmark);

        final Property property = getItem(position);

        province.setText("Provincia: " + property.getProvince());
        city.setText("Citta: " + property.getCity());
        zone.setText("Zona: " + property.getZone());
        rooms.setText("Numero camere: " + property.getRooms());
        bath.setText("Numero bagni: " + property.getBath());
        type.setText("Tipologia: " + property.getType());
        subtype.setText("Sotto tipologia: " + property.getSubtype());
        price.setText("Prezzo: " + property.getPrice() + " €");
        sqm.setText("Mq: " + property.getSqm());
        description.setText("Descrizione: " + property.getDescription());
        foto_link.setText("Link per le foto: " + property.getFoto_link());

        removeBookmark.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {

                JSONObject jo = property.toJSON();

                activity.removeBookmarkConnection(jo);
            }
        });

        return convertView;
    }
}
