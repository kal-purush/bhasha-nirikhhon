package mobile.project.cekjadwaldokter;

import android.content.Intent;
import android.support.annotation.NonNull;
import android.support.v7.app.AppCompatActivity;
import android.os.Bundle;
import android.support.v7.widget.DefaultItemAnimator;
import android.support.v7.widget.GridLayoutManager;
import android.support.v7.widget.RecyclerView;
import android.widget.TextView;

import com.google.firebase.database.DataSnapshot;
import com.google.firebase.database.DatabaseError;
import com.google.firebase.database.DatabaseReference;
import com.google.firebase.database.FirebaseDatabase;
import com.google.firebase.database.ValueEventListener;

import java.util.ArrayList;
import java.util.List;

import mobile.project.cekjadwaldokter.adapterRecyclerView.RecyclerAdapter;
import mobile.project.cekjadwaldokter.adapterRecyclerView.RecyclerAdapter2;
import mobile.project.cekjadwaldokter.paket.firebase.ModelInfoSpesialis;

public class ListInfoDokterSpesialis extends AppCompatActivity {

    List<ModelInfoSpesialis> list;

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_list_info_dokter_spesialis);

        Bundle extras = getIntent().getExtras();
        TextView textView = (TextView) findViewById(R.id.vtext);
        textView.setText(extras.getString("key1"));
        String Temp = extras.getString("key1");

        //display data here
        final RecyclerView recyclerView = (RecyclerView) findViewById(R.id.idSpesialis);

        if (Temp.equals("Spesialis Bedah Saraf")) {
            DatabaseReference reference = FirebaseDatabase.getInstance().getReference().child("List Dokter Spesialis A Dadi");
            reference.keepSynced(true);
            reference.addValueEventListener(new ValueEventListener() {
                @Override
                public void onDataChange(@NonNull DataSnapshot dataSnapshot) {

                    list = new ArrayList<ModelInfoSpesialis>();

                    for (DataSnapshot dataSnapshot1 : dataSnapshot.getChildren()) {
                        ModelInfoSpesialis model = new ModelInfoSpesialis();
                        ModelInfoSpesialis value = dataSnapshot1.getValue(ModelInfoSpesialis.class);
                        String vNamaDokter = value.getNamaDokter();
                        String gambar = value.getImage();
                        String ruangan = value.getRuangan();
                        String hari = value.getHari();
                        String jam = value.getJam();
                        model.setNamaDokter(vNamaDokter);
                        model.setImage(gambar);
                        model.setRuangan(ruangan);
                        model.setHari(hari);
                        model.setJam(jam);
                        list.add(model);
                        RecyclerAdapter2 adapter = new RecyclerAdapter2(list, ListInfoDokterSpesialis.this);
                        RecyclerView.LayoutManager layoutManager = new GridLayoutManager(ListInfoDokterSpesialis.this, 1);
                        recyclerView.setLayoutManager(layoutManager);
                        recyclerView.setItemAnimator(new DefaultItemAnimator());
                        recyclerView.setAdapter(adapter);
                    }

                }

                @Override
                public void onCancelled(@NonNull DatabaseError databaseError) {

                }
            });
        }

    }
}