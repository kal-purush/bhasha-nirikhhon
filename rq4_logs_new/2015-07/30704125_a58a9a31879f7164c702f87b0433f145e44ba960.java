package ru.k.dessel.criminalintent;


import android.content.Context;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONTokener;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.ArrayList;

public class CriminalIntentJSONSerializer {
    private Context mContext;
    private String mFilename;
    public CriminalIntentJSONSerializer(Context c, String f) {
        mContext = c;
        mFilename = f;
    }
    public void saveCrimes(ArrayList<Crime> crimes) throws JSONException, IOException {
        // Построение массива в JSON
        JSONArray array = new JSONArray();
        for (Crime c : crimes)
            array.put(c.toJSON());

        // Запись файла на диск
        Writer writer = null;
        try {
            OutputStream out = mContext								
                    .openFileOutput(mFilename, Context.MODE_PRIVATE);
            writer = new OutputStreamWriter(out);
            writer.write(array.toString());
        } finally {
            if (writer != null)
                writer.close();
        }
    }

    public ArrayList<Crime> loadCrimes() throws IOException, JSONException {
        ArrayList<Crime> crimes = new ArrayList<Crime>();
        BufferedReader reader  = null;

        try {
            // Открытие и чтение файла в StringBuilder
            InputStream in = mContext.openFileInput(mFilename);
            reader = new BufferedReader(new InputStreamReader(in));

            String line = null;
            StringBuilder jsonString = new StringBuilder();

            while ((line = reader.readLine()) != null) {
                jsonString.append(line);
            }

            // Разбор JSON с использованием JSONTokener
            JSONArray array = (JSONArray) new JSONTokener(jsonString.toString()).nextValue();
            // Построение массива объектов Crime по данным JSONObject
            for (int i = 0; i < array.length(); i++) {
                crimes.add(new Crime(array.getJSONObject(i)));
            }

        }
         catch (FileNotFoundException e) {
            //
        } finally {
            if (reader != null)
                reader.close();
        }

        return  crimes;
    }
}