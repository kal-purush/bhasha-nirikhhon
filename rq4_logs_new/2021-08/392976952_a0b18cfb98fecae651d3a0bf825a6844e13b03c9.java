package com.example.seungho2;

import androidx.appcompat.app.AppCompatActivity;

import android.app.TabActivity;
import android.content.Intent;
import android.graphics.Color;
import android.os.Bundle;
import android.view.View;
import android.widget.Button;
import android.widget.TabHost;
import android.widget.TextView;
import android.widget.Toast;

import java.util.ArrayList;

@SuppressWarnings("deprecation")
public class AddActivity extends TabActivity {

    TextView tv1, tv2, tv3, tv4, tv5, tv6, tv7, tv8;

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_add);



//    String str1 = null;

//    Button[] btnId = new Button[]{findViewById(R.id.btnA_1), findViewById(R.id.btnB_1), findViewById(R.id.btnC_1)
//                               ,findViewById(R.id.btnD_1), findViewById(R.id.btnE_1), findViewById(R.id.btnF_1)
//                               ,findViewById(R.id.btnG_1)};

        TabHost tabHost = getTabHost();

        TabHost.TabSpec tabspecCoffee = tabHost.newTabSpec("COFFEE").setIndicator("커피");
        tabspecCoffee.setContent(R.id.tabCoffee);
        tabHost.addTab(tabspecCoffee);

        TabHost.TabSpec tabspecTea = tabHost.newTabSpec("TEA").setIndicator("차");
        tabspecTea.setContent(R.id.tabTea);
        tabHost.addTab(tabspecTea);

        TabHost.TabSpec tabspecAde = tabHost.newTabSpec("ADE").setIndicator("에이드\n&주스");
        tabspecAde.setContent(R.id.tabAde);
        tabHost.addTab(tabspecAde);

        TabHost.TabSpec tabspecBasket = tabHost.newTabSpec("BASKET").setIndicator("장바구니");
        tabspecBasket.setContent(R.id.tabBasket);
        tabHost.addTab(tabspecBasket);

        tabHost.setCurrentTab(0);

    }

    public void mOnClick1(View view) {
        tv1 = (TextView)findViewById(R.id.tv1);
        tv2 = (TextView)findViewById(R.id.tv2);
        tv3 = (TextView)findViewById(R.id.tv3);
        tv4 = (TextView)findViewById(R.id.tv4);
        tv5 = (TextView)findViewById(R.id.tv5);
        tv6 = (TextView)findViewById(R.id.tv6);
        tv7 = (TextView)findViewById(R.id.tv7);
        tv8 = (TextView)findViewById(R.id.tv8);

        if (view.getId() == R.id.btnA_1) {
            tv1.setText("아아");
            tv1.setBackgroundColor(Color.parseColor("#A5A2A7"));
        } else if (view.getId() == R.id.btnB_1) {
            tv2.setText("뜨아");
            tv2.setBackgroundColor(Color.parseColor("#A5A2A7"));
        }else if (view.getId() == R.id.btnC_1) {
            tv3.setText("카페라떼");
            tv3.setBackgroundColor(Color.parseColor("#A5A2A7"));
        }else if (view.getId() == R.id.btnD_1) {
            tv4.setText("카페모카");
            tv4.setBackgroundColor(Color.parseColor("#A5A2A7"));
        }else if (view.getId() == R.id.btnE_1) {
            tv5.setText("카푸치노");
            tv5.setBackgroundColor(Color.parseColor("#A5A2A7"));
        }else if (view.getId() == R.id.btnF_1) {
            tv6.setText("콜드브루");
            tv6.setBackgroundColor(Color.parseColor("#A5A2A7"));
        }else if (view.getId() == R.id.btnG_1) {
            tv7.setText("마키아또");
            tv7.setBackgroundColor(Color.parseColor("#A5A2A7"));
        }
        Toast.makeText(getApplicationContext(), "장바구니에 추가되었습니다.", Toast.LENGTH_SHORT).show();
    }

    public void mOnClick2(View view) {
        tv1 = (TextView)findViewById(R.id.tv1);
        tv2 = (TextView)findViewById(R.id.tv2);
        tv3 = (TextView)findViewById(R.id.tv3);
        tv4 = (TextView)findViewById(R.id.tv4);
        tv5 = (TextView)findViewById(R.id.tv5);
        tv6 = (TextView)findViewById(R.id.tv6);
        tv7 = (TextView)findViewById(R.id.tv7);
        tv8 = (TextView)findViewById(R.id.tv8);

        if (view.getId() == R.id.btnA_2) {
            tv1.setText("녹차");
            tv1.setBackgroundColor(Color.parseColor("#A5A2A7"));
        } else if (view.getId() == R.id.btnB_2) {
            tv2.setText("페퍼민트");
            tv2.setBackgroundColor(Color.parseColor("#A5A2A7"));
        }else if (view.getId() == R.id.btnC_2) {
            tv3.setText("캐모마일");
            tv3.setBackgroundColor(Color.parseColor("#A5A2A7"));
        }else if (view.getId() == R.id.btnD_2) {
            tv4.setText("레몬");
            tv4.setBackgroundColor(Color.parseColor("#A5A2A7"));
        }else if (view.getId() == R.id.btnE_2) {
            tv5.setText("얼그레이");
            tv5.setBackgroundColor(Color.parseColor("#A5A2A7"));
        }else if (view.getId() == R.id.btnF_2) {
            tv6.setText("유자");
            tv6.setBackgroundColor(Color.parseColor("#A5A2A7"));
        }else if (view.getId() == R.id.btnG_2) {
            tv7.setText("자몽");
            tv7.setBackgroundColor(Color.parseColor("#A5A2A7"));
        }else if (view.getId() == R.id.btnH_2) {
            tv8.setText("사과유자");
            tv8.setBackgroundColor(Color.parseColor("#A5A2A7"));
        }
        Toast.makeText(getApplicationContext(), "장바구니에 추가되었습니다.", Toast.LENGTH_SHORT).show();
    }
    public void mOnClick3(View view) {
        tv1 = (TextView)findViewById(R.id.tv1);
        tv2 = (TextView)findViewById(R.id.tv2);
        tv3 = (TextView)findViewById(R.id.tv3);
        tv4 = (TextView)findViewById(R.id.tv4);
        tv5 = (TextView)findViewById(R.id.tv5);
        tv6 = (TextView)findViewById(R.id.tv6);
        tv7 = (TextView)findViewById(R.id.tv7);
        tv8 = (TextView)findViewById(R.id.tv8);

        tv1.setText("");
        tv1.setBackgroundColor(Color.parseColor("#FFFFFF"));
        tv2.setText("");
        tv2.setBackgroundColor(Color.parseColor("#FFFFFF"));
        tv3.setText("");
        tv3.setBackgroundColor(Color.parseColor("#FFFFFF"));
        tv4.setText("");
        tv4.setBackgroundColor(Color.parseColor("#FFFFFF"));
        tv5.setText("");
        tv5.setBackgroundColor(Color.parseColor("#FFFFFF"));
        tv6.setText("");
        tv6.setBackgroundColor(Color.parseColor("#FFFFFF"));
        tv7.setText("");
        tv7.setBackgroundColor(Color.parseColor("#FFFFFF"));
        tv8.setText("");
        tv8.setBackgroundColor(Color.parseColor("#FFFFFF"));
        Toast.makeText(getApplicationContext(), "삭제되었습니다.", Toast.LENGTH_SHORT).show();
    }
}