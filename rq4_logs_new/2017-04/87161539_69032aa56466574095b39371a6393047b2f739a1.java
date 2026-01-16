package com.dnd.pololef.dndcharacter.Screen.EditCharacter;

import android.app.Activity;
import android.app.AlertDialog;
import android.content.DialogInterface;
import android.os.Bundle;
import android.view.View;
import android.widget.ArrayAdapter;
import android.widget.Button;

import com.dnd.pololef.dndcharacter.R;

/**
 * Created by Pololef on 2017/2/28.
 */

public class EditCharacterActivity extends Activity {

    private int mCurrentPoint;

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_edit_character);

        //region 設定職業選擇
        Button careerButton = (Button)findViewById(R.id.button_select_career);
        careerButton.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                AlertDialog.Builder builderSingle = new AlertDialog.Builder(EditCharacterActivity.this);
                builderSingle.setTitle("Select One Name:-");

                final ArrayAdapter<String> arrayAdapter = new ArrayAdapter<>(EditCharacterActivity.this, android.R.layout.select_dialog_singlechoice);
                arrayAdapter.add("Hardik");
                arrayAdapter.add("Archit");
                arrayAdapter.add("Jignesh");
                arrayAdapter.add("Umang");
                arrayAdapter.add("Gatti");

                builderSingle.setNegativeButton("cancel", new DialogInterface.OnClickListener() {
                    @Override
                    public void onClick(DialogInterface dialog, int which) {
                        dialog.dismiss();
                    }
                });

                builderSingle.setAdapter(arrayAdapter, new DialogInterface.OnClickListener() {
                    @Override
                    public void onClick(DialogInterface dialog, int which) {
                        String strName = arrayAdapter.getItem(which);

                    }
                });
                builderSingle.show();
            }
        });
        Button raceButton = (Button)findViewById(R.id.button_select_race);

        //endregion

        /*
        屬性值　　需要花費點數 　　
        ８　　　０ 　　
        ９　　　１ 　
        １０　　　２ 　
        １１　　　３ 　
        １２　　　４ 　
        １３　　　５ 　
        １４　　　６ 　
        １５　　　８ 　
        １６　　１０ 　
        １７　　１３ 　
        １８　　１６
         */
        mCurrentPoint = 40;
    }
}