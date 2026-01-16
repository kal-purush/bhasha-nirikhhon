package android.snapevent;

import android.content.Context;
import android.content.Intent;
import android.view.View;

/**
 * Created by ser on 2015/10/30.
 */
public class MapLogOnClickListener implements View.OnClickListener {

    MapsActivity.MarkerItem placeitem;
    Context activity;
    public MapLogOnClickListener(MapsActivity.MarkerItem it,Context activity) {
        placeitem=it;
        this.activity=activity;
    }

    @Override
    public void onClick(View v) {
        Intent i = new Intent(activity,ShowEventActivity.class);
        if(placeitem!=null) {
            if (placeitem.getBean().getLink().getUrl().substring(0, 9).equals("<![CDATA[")) {
                i.putExtra("url", placeitem.getBean().getLink().getUrl().substring(9, placeitem.getBean().getLink().getUrl().length() - 3));

            } else {
                i.putExtra("url", placeitem.getBean().getLink().getUrl());
            }
            i.putExtra("title", placeitem.getBean().getTitle());
        }
        activity.startActivity(i); //start intent to Activity
    }
}