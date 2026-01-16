package com.example.duan1_catmusic.Activity;

import android.content.Intent;
import android.os.Bundle;
import android.view.View;
import android.widget.Button;
import android.widget.SearchView;

import androidx.activity.EdgeToEdge;
import androidx.appcompat.app.AppCompatActivity;
import androidx.core.graphics.Insets;
import androidx.core.view.ViewCompat;
import androidx.core.view.WindowInsetsCompat;
import androidx.recyclerview.widget.GridLayoutManager;
import androidx.recyclerview.widget.LinearLayoutManager;
import androidx.recyclerview.widget.RecyclerView;

import com.example.duan1_catmusic.Adapter.MusicAdapter;
import com.example.duan1_catmusic.Adapter.ThuVien_NgheSi_Adapter;
import com.example.duan1_catmusic.Adapter.casiAdapter;
import com.example.duan1_catmusic.Adapter.thuvienAdapter;
import com.example.duan1_catmusic.DAO.casiDAO;
import com.example.duan1_catmusic.DAO.nhacDAO;
import com.example.duan1_catmusic.R;
import com.example.duan1_catmusic.model.Casi;
import com.example.duan1_catmusic.model.Nhac;

import java.util.ArrayList;
import java.util.List;

public class Screen_ThuVien_NgheSi extends AppCompatActivity {


    private com.example.duan1_catmusic.DAO.casiDAO casiDAO;
    private RecyclerView rcvnghesi, rcvtop;
    private com.example.duan1_catmusic.DAO.nhacDAO nhacDAO;
    private MusicAdapter adapternhac;
    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_screen_thu_vien_nghe_si);

        SearchView edt_timkiem = findViewById(R.id.edt_timkiem);
        rcvnghesi = findViewById(R.id.rcvnghesi);
//        btn_allSong = findViewById(R.id.btn_allSong);

        casiDAO = new casiDAO(this);
        ArrayList<Casi> list = casiDAO.getcasi();
        GridLayoutManager manager = new GridLayoutManager(this, 2);
//        manager.setOrientation(RecyclerView.VERTICAL);
        rcvnghesi.setLayoutManager(manager);
        ThuVien_NgheSi_Adapter adapter = new ThuVien_NgheSi_Adapter(this,list);
        rcvnghesi.setAdapter(adapter);

        nhacDAO = new nhacDAO(this);
        List<Nhac> listnhac = nhacDAO.getSongArtistList();

        edt_timkiem.setOnQueryTextListener(new SearchView.OnQueryTextListener() {
            @Override
            public boolean onQueryTextSubmit(String query) {
                adapter.getFilter().filter(query);
                return false;
            }

            @Override
            public boolean onQueryTextChange(String newText) {
                adapter.getFilter().filter(newText);
                return true;
            }
        });




    }
}