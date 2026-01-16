package com.example.safety_qr.activity;

import android.os.Bundle;

import androidx.appcompat.app.AppCompatActivity;

import androidx.recyclerview.widget.LinearLayoutManager;
import androidx.recyclerview.widget.RecyclerView;

import com.example.safety_qr.R;
import com.example.safety_qr.adapter.RecyclerAdapter;
import com.example.safety_qr.domain.History;
import com.example.safety_qr.infrastructure.SQLiteHelper;

import java.util.List;

public class History_Activity extends AppCompatActivity {

    SQLiteHelper dbHelper;
    List<History> history;
    RecyclerView recyclerView;
    RecyclerAdapter recyclerAdapter;

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_history);

        dbHelper = new SQLiteHelper(History_Activity.this);
        history = dbHelper.selectALL();


        for(int i = 0; i < history.size(); i++){
            System.out.println("history list : " + history.get(i));
        }

        recyclerView = findViewById(R.id.recyclerview);
        LinearLayoutManager linearLayoutManager = new LinearLayoutManager(
                History_Activity.this, RecyclerView.VERTICAL, false);

        recyclerView.setLayoutManager(linearLayoutManager);


        recyclerAdapter = new RecyclerAdapter(history);
        recyclerView.setAdapter(recyclerAdapter);
        recyclerAdapter.notifyDataSetChanged();
        //recyclerAdapter.addItem(history);


    }
/*
    class RecyclerAdapter extends RecyclerView.Adapter<RecyclerAdapter.ItemViewHolder>{
        public List<History> listdata;

        class ItemViewHolder extends RecyclerView.ViewHolder{

            private final TextView url;
            private TextView num;

            public ItemViewHolder(@NonNull View itemView){
                super(itemView);



                url = itemView.findViewById(R.id.item_url);

                itemView.setOnLongClickListener(new View.OnLongClickListener() {
                    @Override
                    public boolean onLongClick(View view) {

                        AlertDialog.Builder builder = new AlertDialog.Builder(HistoryActivity.this);

                        builder.setTitle("삭제").setMessage("삭제하시겠습니까?");

                        builder.setPositiveButton("취소", new DialogInterface.OnClickListener(){
                            @Override
                            public void onClick(DialogInterface dialog, int id)
                            {
                                //Toast.makeText(getApplicationContext(), "OK Click", Toast.LENGTH_SHORT).show();
                            }
                        });

                        builder.setNegativeButton("삭제", new DialogInterface.OnClickListener(){
                            @Override
                            public void onClick(DialogInterface dialog, int id)
                            {
                                int position = getAdapterPosition();
                                int seq = (int)url.getTag();

                                if(position != RecyclerView.NO_POSITION){
                                    dbHelper.deleteMemo(seq);
                                    removeItem(position);
                                    notifyDataSetChanged();
                                }

                                Toast.makeText(getApplicationContext(), "삭제되었습니다.", Toast.LENGTH_SHORT).show();
                            }
                        });



                        AlertDialog alertDialog = builder.create();
                        alertDialog.show();


                        return false;
                    }
                });


            }

        }

        public RecyclerAdapter(List<History> listdata){
            this.listdata = listdata;
        }

        @NonNull
        @Override
        public ItemViewHolder onCreateViewHolder(@NonNull ViewGroup viewGroup, int i) {

            View view = LayoutInflater.from(viewGroup.getContext()).inflate(R.layout.item_history, viewGroup, false);

            return new ItemViewHolder(view);
        }

        @Override
        public int getItemCount() {
            return listdata.size();
        }

        @Override
        public void onBindViewHolder(@NonNull ItemViewHolder itemViewHolder, int i) {
            History history = listdata.get(i);


            itemViewHolder.url.setText(history.getUrl());


        }
        void addItem(History history) {
            listdata.add(history);
        }

        void removeItem(int position){
            listdata.remove(position);

        }




    }*/
}
package com.example.safety_qr.activity;
import com.example.safety_qr.R;

public class Intro_Activity extends AppCompatActivity {
                Intent intent = new Intent(getApplicationContext(), Main_Activity.class);
package com.example.safety_qr.activity;
import com.example.safety_qr.R;
import com.example.safety_qr.infrastructure.ScanQR;
import com.example.safety_qr.infrastructure.Client;
public class Main_Activity extends AppCompatActivity {
        Intent intent = new Intent(this, SearchQR_Activity.class);
    public void goHistory(View view) {
        Intent intent = new Intent(this, History_Activity.class);
        startActivity(intent);
    }
package com.example.safety_qr.activity;
import com.example.safety_qr.R;
import com.example.safety_qr.infrastructure.ScanQR;
import com.example.safety_qr.domain.History;
import com.example.safety_qr.infrastructure.SQLiteHelper;

import java.util.List;


public class ScanResult_Activity extends AppCompatActivity {

    SQLiteHelper dbHelper;
    List<History> history;
        // <--- DB --->
        dbHelper = new SQLiteHelper(ScanResult_Activity.this);
        // 악성 유무값 toString
        String result = Integer.toString(percent);
        History history = new History(url, result);
        // DB insert
        dbHelper.insertHistory(history);
        // <-- DB END -- >


package com.example.safety_qr.activity;
import com.example.safety_qr.R;
import com.example.safety_qr.infrastructure.Client;

public class SearchQR_Activity extends AppCompatActivity {
            //Intent url = new Intent(this, ScanResult_Activity.class);
            //url.putExtra("result", 0);