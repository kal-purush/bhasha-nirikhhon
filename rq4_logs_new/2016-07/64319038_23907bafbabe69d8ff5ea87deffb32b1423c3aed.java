package bd.edu.ibaisuniv.ibaisuniversity.customicontab;

import android.os.Bundle;
import android.support.annotation.Nullable;
import android.support.v4.app.Fragment;
import android.view.LayoutInflater;
import android.view.View;
import android.view.ViewGroup;

import bd.edu.ibaisuniv.ibaisuniversity.customicontab.AFragment;
import bd.edu.ibaisuniv.ibaisuniversity.customicontab.R;

/**
 * Created by mbiplobe on 7/29/2016.
 */
public class TestFragment extends Fragment {

    @Override
    public View onCreateView(LayoutInflater inflater, ViewGroup container,
                             Bundle savedInstanceState) {
        View view = inflater.inflate(R.layout.test_frament_layout, container, false);
        getFragmentManager().beginTransaction().add(R.id.testFrame, new AFragment()).commit();
        return view;
    }

    @Override
    public void onActivityCreated(@Nullable Bundle savedInstanceState) {
        super.onActivityCreated(savedInstanceState);
    }
}