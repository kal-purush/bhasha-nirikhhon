package com.easyeat;

import android.content.Intent;
import android.os.Bundle;
import android.view.View;
import android.view.ViewGroup;
import android.widget.Button;
import android.widget.ListView;

import com.easyeat.adapter.BankCardAdapter;

public class BankCardActivity extends BaseActivity implements View.OnClickListener {
    private ListView lv_cards;
    private Button bt_add;
    private BankCardAdapter adapter;

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_bank_card);
        initView();
    }


    private void initView() {
        lv_cards = (ListView) findViewById(R.id.lv_cards);
        adapter = new BankCardAdapter(this, EasyEatApplication.getCurrentUser().paymentInfo);

        bt_add = (Button) findViewById(R.id.bt_add);
        bt_add.setOnClickListener(this);
    }

    @Override
    protected void onResume() {
        super.onResume();
        initData();
    }

    private void initData() {
        adapter.setPayments(EasyEatApplication.getCurrentUser().paymentInfo);
        lv_cards.setAdapter(adapter);
        ViewGroup.LayoutParams params = lv_cards.getLayoutParams();
        params.height = EasyEatApplication.getCurrentUser().paymentInfo.length *
                getResources().getDimensionPixelSize(R.dimen.item_restaurant_height);
    }

    @Override
    public void onClick(View view) {
        switch (view.getId()) {
            case R.id.bt_add:
                gotoAddBankCardActivity();
                break;
        }
    }

    private void gotoAddBankCardActivity() {
        Intent intent = new Intent(this, AddBankCardActivity.class);
        startActivity(intent);
    }
}