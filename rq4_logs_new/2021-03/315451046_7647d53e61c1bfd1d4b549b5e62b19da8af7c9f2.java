package com.example.pacmanlike;

import androidx.appcompat.app.AppCompatActivity;

import android.content.Context;
import android.os.Bundle;

import java.io.File;
import java.io.FileWriter;
import java.io.InputStream;
import java.util.Scanner;

public class LevelMakerActivity extends AppCompatActivity {

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_level_maker);

        copyFileToStorage();
    }

    private void copyFileToStorage(){
        writeFileOnInternalStorage("thedefaultmap.csv");
    }

    public void writeFileOnInternalStorage(String sFileName){
        try {
            InputStream stream = getAssets().open(SelectionScreen.BASIC_MAP);
            Scanner reader = new Scanner(stream);

            File file = new File(getApplicationContext().getFilesDir(), sFileName);
            FileWriter writer = new FileWriter(file);

            while (reader.hasNextLine()) {
                String line = reader.nextLine();
                writer.write(line);
                writer.write("\n");
            }

            writer.flush();
            writer.close();
        } catch (Exception e){
            e.printStackTrace();
            super.finish();
        }
    }
}