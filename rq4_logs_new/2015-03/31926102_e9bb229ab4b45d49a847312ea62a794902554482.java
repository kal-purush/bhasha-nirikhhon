package co.johnnyli.gamer;

import android.os.Bundle;
import android.support.v4.app.Fragment;
import android.view.LayoutInflater;
import android.view.View;
import android.view.ViewGroup;
import android.widget.TextView;

/**
 * Created by johnnyli on 3/6/15.
 */
public class AboutFragment extends Fragment {

    private TextView name;
    private TextView description;

    @Override
    public View onCreateView(LayoutInflater inflater, ViewGroup container, Bundle savedInstanceState) {
        View view = inflater.inflate(R.layout.about_fragment, container, false);
        name = (TextView) view.findViewById(R.id.group_name);
        description = (TextView) view.findViewById(R.id.group_description);
        return view;
    }

    @Override
    public void onActivityCreated(Bundle savedInstanceState) {
        super.onActivityCreated(savedInstanceState);
        name.setText("Avalon Group");
        description.setText("This is the description for the Avalon group. blah blah blah blah" +
                "blah blah blah blah blah blah blah blah blah blah blah blah blah blah blah");

    }
}
