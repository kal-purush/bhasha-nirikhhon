package com.maciek.facebooktest.Workspace;

import android.app.Dialog;
import android.content.Intent;
import android.support.v7.app.AppCompatActivity;
import android.os.Bundle;
import android.text.Layout;
import android.util.Log;
import android.view.View;
import android.view.WindowManager;
import android.widget.Button;

import com.maciek.facebooktest.R;

public class RegistrationBackground extends AppCompatActivity implements View.OnClickListener {

    public final String TAG = "User_action";

    Button dialogNext;
    Button dialogBack;

    private Dialog dialog;

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_registration_background);
        openDialog();

    }


    public void openDialog() {
        dialog = new Dialog(RegistrationBackground.this);
        dialog.setContentView(R.layout.dialog_three_options);

        WindowManager.LayoutParams lp = new WindowManager.LayoutParams();
        lp.copyFrom(dialog.getWindow().getAttributes());
        lp.width = WindowManager.LayoutParams.MATCH_PARENT;
        lp.height = WindowManager.LayoutParams.WRAP_CONTENT;
        dialog.show();
        dialog.getWindow().setAttributes(lp);

        Button hasSpot = (Button) dialog.findViewById(R.id.dialogHaveSpot);
        Button wantSpot = (Button) dialog.findViewById(R.id.dialogFindSpot);
        Button hasBoth = (Button) dialog.findViewById(R.id.dialogBoth);

        hasSpot.setOnClickListener(this);
        wantSpot.setOnClickListener(this);
        hasBoth.setOnClickListener(this);
        dialog.setCanceledOnTouchOutside(false);

    }


    @Override
    public void onClick(View v) {
        switch (v.getId()) {

            case R.id.dialogHaveSpot:
                dialog.dismiss();
                openDialog(R.layout.dialog_has_spot);
                break;

            case R.id.dialogBoth:
                dialog.dismiss();
                openDialog(R.layout.dialog_has_spot);
                break;

            case R.id.dialogFindSpot:
                dialog.dismiss();
                openDialog(R.layout.dialog_find_spot);
                break;

            case R.id.dialog_has_spot_back:
                dialog.dismiss();
                openDialog();
                break;

            case R.id.dialog_has_spot_forward:
                startActivity(new Intent(RegistrationBackground.this, SpotMenu.class));
                break;

        }
    }

    public void openDialog(int layout) {
        dialog = new Dialog(RegistrationBackground.this);
        dialog.setContentView(layout);
        WindowManager.LayoutParams lp = new WindowManager.LayoutParams();
        lp.copyFrom(dialog.getWindow().getAttributes());
        lp.width = WindowManager.LayoutParams.MATCH_PARENT;
        lp.height = WindowManager.LayoutParams.WRAP_CONTENT;
        dialog.show();
        dialog.getWindow().setAttributes(lp);

        dialogBack = (Button) dialog.findViewById(R.id.dialog_has_spot_back);
        dialogNext = (Button) dialog.findViewById(R.id.dialog_has_spot_forward);

        dialogBack.setOnClickListener(this);
        dialogNext.setOnClickListener(this);

        dialog.setCanceledOnTouchOutside(false);

    }

}