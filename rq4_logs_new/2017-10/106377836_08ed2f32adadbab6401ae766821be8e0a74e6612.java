package finalproject_plants;

import android.app.Fragment;
import android.os.Bundle;
import android.support.annotation.Nullable;
import android.view.LayoutInflater;
import android.view.View;
import android.view.ViewGroup;
import android.widget.TextView;

import finalproject_plants.R;

/**
 * Created by hugh on 2017/10/15.
 */

    public class FirstFragment extends Fragment {
    private String context;
    private TextView mTextView;

    public FirstFragment(){}

    public void set (String cont) {
        this.context = cont;
    }


    @Nullable
    @Override
    public View onCreateView(LayoutInflater inflater, ViewGroup container, Bundle savedInstanceState) {
        View view = inflater.inflate(R.layout.first_fragment, container, false);
        mTextView = view.findViewById(R.id.txt_content);
        //mTextView = (TextView)getActivity().findViewById(R.id.txt_content);
        mTextView.setText(context);
        return view;
    }
}