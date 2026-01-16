/**
 * Created by drewheckathorn on 12/8/16.
 */

package com.clubjevin.festivus;

import android.app.Activity;
import android.app.Dialog;
import android.os.Bundle;
import android.view.View;
import android.view.Window;
import android.widget.Button;
import android.widget.TextView;


import org.w3c.dom.Text;

import com.clubjevin.festivus.data.Grievance;
import com.clubjevin.festivus.data.GrievancesDAO;

public class verifyTextToSpeech extends Dialog implements
        android.view.View.OnClickListener {

    public MainActivity c;
    public Dialog d;
    public Button yes, no;
    public TextView t;
    public String grvString;
    public GrievancesDAO local_DAO;

    public verifyTextToSpeech(MainActivity a, String str) {
        super(a);
        // TODO Auto-generated constructor stub
        this.c = a;
        this.grvString = str;
    }

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        requestWindowFeature(Window.FEATURE_NO_TITLE);
        setContentView(R.layout.verify_text_to_speech_popup);
        yes = (Button) findViewById(R.id.btn_yes);
        no = (Button) findViewById(R.id.btn_no);
        yes.setOnClickListener(this);
        no.setOnClickListener(this);
        t = (TextView) findViewById(R.id.txt_dia);
        t.setText("Did you say?\n"+grvString);

    }

    @Override
    public void onClick(View v) {
        switch (v.getId()) {
            case R.id.btn_yes:
                c.getDao().insert(new Grievance(System.currentTimeMillis(), grvString));
                //c.finish();
                dismiss();
                break;
            case R.id.btn_no:
                dismiss();
                break;
            default:
                break;
        }
        dismiss();
    }
}