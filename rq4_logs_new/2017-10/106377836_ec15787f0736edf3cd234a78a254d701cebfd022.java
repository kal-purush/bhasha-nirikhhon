package finalproject_plants;

import android.app.Fragment;
import android.content.Intent;
import android.os.Bundle;
import android.support.annotation.Nullable;
import android.view.LayoutInflater;
import android.view.View;
import android.view.ViewGroup;
import android.widget.ImageButton;


public class noteclass extends Fragment {
    private ImageButton phobtn;
    private ImageButton plabtn;
    private ImageButton jacbtn;
    private ImageButton hanbtn;
    private ImageButton linbtn;
    @Nullable
    @Override
    public View onCreateView(LayoutInflater inflater, ViewGroup container, Bundle savedInstanceState) {
        View view = inflater.inflate(R.layout.note, container, false);
        phobtn = view.findViewById(R.id.photinia);
        phobtn.setOnClickListener(new View.OnClickListener(){
            @Override
            public void onClick(View v) {
                Intent intent = new Intent(noteclass.this.getActivity(),photinia.class);
                startActivity(intent);
            }
        });
        plabtn = view.findViewById(R.id.planch);
        plabtn.setOnClickListener(new View.OnClickListener(){
            @Override
            public void onClick(View v) {
                Intent intent = new Intent(noteclass.this.getActivity(),planch.class);
                startActivity(intent);
            }
        });
        jacbtn = view.findViewById(R.id.jacq);
        jacbtn.setOnClickListener(new View.OnClickListener(){
            @Override
            public void onClick(View v) {
                Intent intent = new Intent(noteclass.this.getActivity(),jacq.class);
                startActivity(intent);
            }
        });
        hanbtn = view.findViewById(R.id.hance);
        hanbtn.setOnClickListener(new View.OnClickListener(){
            @Override
            public void onClick(View v) {
                Intent intent = new Intent(noteclass.this.getActivity(),hance.class);
                startActivity(intent);
            }
        });
        linbtn = view.findViewById(R.id.linn);
        linbtn.setOnClickListener(new View.OnClickListener(){
            @Override
            public void onClick(View v) {
                Intent intent = new Intent(noteclass.this.getActivity(),linn.class);
                startActivity(intent);
            }
        });
        return view;
    }
}