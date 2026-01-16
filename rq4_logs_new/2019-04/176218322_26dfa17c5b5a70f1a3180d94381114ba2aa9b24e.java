package com.example.lui_project.Fragment;

import android.app.Activity;
import android.content.Context;
import android.content.Intent;
import android.database.Cursor;
import android.graphics.Bitmap;
import android.graphics.BitmapFactory;
import android.os.Bundle;
import android.support.annotation.Nullable;
import android.support.v4.app.Fragment;
import android.util.Log;
import android.view.LayoutInflater;
import android.view.View;
import android.view.ViewGroup;
import android.widget.AdapterView;
import android.widget.ListView;
import android.widget.Toast;

import com.example.lui_project.R;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.example.lui_project.PlayActivity;
import com.example.lui_project.adapter.MyAdapter;
import com.example.lui_project.db.DatasDao;
import com.example.lui_project.Test_findActivity;

public class FindFragment extends Fragment {
    private View view;//界面的布局
    private Context context;
    public static Bitmap[] bitmaps = new Bitmap[5];
    public static String[] warm_up_exercise = new String[5];

    public static final String cishu = "1組6次";
    private ListView listView;
    private List<Map<String,Object>> list;
    @Override
    public void onAttach(Context context) {
        super.onAttach(context);
        this.context = context;
    }

    @Nullable
    @Override
    public View onCreateView(LayoutInflater inflater, ViewGroup container, Bundle savedInstanceState) {
        view = inflater.inflate(R.layout.fragment_find,null);
        listView = (ListView) view.findViewById(R.id.find_list);

        if (isAdded()){
            list = new ArrayList<>();
            for (int i =0 ; i< 5 ; i++){
                Map<String,Object> map =new HashMap<>();
                warm_up_exercise[0]="俯身啞鈴飛鳥";
                warm_up_exercise[1]="俯臥撑";
                warm_up_exercise[2]="滾輪支點俯臥撑";
                warm_up_exercise[3]="平板卧推";
                warm_up_exercise[4]="仰臥平板槓鈴肱三彎舉";
                map.put("tu",Test_findActivity.bitmaps[i]);
                map.put("xm",warm_up_exercise[i]);
                map.put("cs",cishu);
                map.put("tj","增加運動計畫");
                list.add(map);
            }
            MyAdapter adapter = new MyAdapter(context,list,this);
            listView.setAdapter(adapter);
            DatasDao datasDao = new DatasDao(getContext());
            Cursor cursor = datasDao.selectAll("plans");
            Log.e("當前運動任務數目",cursor.getCount() + "個");
        }


        listView.setOnItemClickListener(new AdapterView.OnItemClickListener() {
            @Override
            public void onItemClick(AdapterView<?> parent, View view, int position, long id) {
                Toast.makeText(context,"開始運動", Toast.LENGTH_SHORT).show();
                startActivity(new Intent(getContext(), PlayActivity.class).putExtra("play_type", position).putExtra("what",1));
            }
        });
        return view;
    }


}