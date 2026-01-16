package ar.uba.kanji;

import android.app.Activity;
import android.content.res.AssetManager;
import android.os.Bundle;
import android.view.KeyEvent;
import android.view.inputmethod.EditorInfo;
import android.widget.EditText;
import android.widget.ListView;
import android.widget.TextView;
import android.widget.Toast;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Vector;

public class DictionaryActivity extends Activity {

    private HashMap<String, String[]> dic;

    @Override
    public void onCreate(final Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        dic = new HashMap<>();
        setContentView(R.layout.dictionary);
        createDictionary();

        final EditText mEdit   = (EditText)findViewById(R.id.searchkey);

        mEdit.setOnEditorActionListener(new TextView.OnEditorActionListener() {
            @Override
            public boolean onEditorAction(TextView v, int actionId, KeyEvent event) {
                if (actionId == EditorInfo.IME_ACTION_SEARCH) {
                    performSearch(mEdit.getText().toString());
                    return true;
                }
                return false;
            }
        });
    }

    private void performSearch(String c) {

        Vector<String> labels = new Vector<String>();
        AssetManager am = getApplicationContext().getAssets();


        try {
            InputStream is = am.open("lista.txt");
            BufferedReader br = new BufferedReader(new InputStreamReader(is));
            String line;
            while ((line = br.readLine()) != null) {
                labels.add(line);
            }
            br.close();
        } catch (IOException e) {
            throw new RuntimeException("Problem reading label file!" , e);
        }


        List<Integer> list = getResultList(c);

        ArrayList<Kanji> arrayOfKanji = new ArrayList<Kanji>();
        for (int i=0; i<list.size(); i++){

            String[] parts = labels.get(list.get(i)).split(",");
            arrayOfKanji.add(new Kanji(parts[0],parts[5],parts[4],parts[3],parts[2], parts[1],1.0f));
        }

        KanjiAdapter adapter = new KanjiAdapter(this, arrayOfKanji);

        ListView listView = (ListView) findViewById(R.id.listviewresult);
        listView.setAdapter(adapter);

    }

    private  List<Integer>  getResultList(String stringToSearch) {

        String c = stringToSearch;
        if ( stringToSearch.contains("\n") ) {
            c = stringToSearch.substring(0,stringToSearch.length()-1);
        }

        List<Integer> result = new ArrayList<Integer>();

        List<Integer> possibleResults = new ArrayList<Integer>() ;
        List<List<Integer>> resultsContainingWords =  new ArrayList<>();

        String[] wordsToSearch = c.split(" ");


        for (int i=0; i<wordsToSearch.length;i++){
            String word =wordsToSearch[i];
            if (dic.containsKey(word)) {
                List<Integer> res = new ArrayList<Integer>();
                String[] sres = dic.get(word);
                for (int j=0; j<sres.length;j++){
                    Integer integ = new Integer(Integer.parseInt(sres[j]));
                    res.add(integ);
                    possibleResults.add(integ);
                    result.add(integ);
                }
                resultsContainingWords.add (res);
            }
        }

        for (int n= 0; n<possibleResults.size();n++){
            for (int x=0;x<resultsContainingWords.size();x++){
                List<Integer> r  =resultsContainingWords.get(x);
                if (!r.contains(possibleResults.get(n))){
                    result.remove(possibleResults.get(n));
                }
            }
        }

        //Remove duplicates
        HashSet<Integer> hashSet = new HashSet<Integer>();
        hashSet.addAll(result);
        result.clear();
        result.addAll(hashSet);
        return result;

    }


    private void createDictionary() {
        AssetManager am = getApplicationContext().getAssets();
        try {
          InputStream is = am.open("dictionary.txt");
          BufferedReader r = new BufferedReader(new InputStreamReader(is));
          String line;
          while ((line = r.readLine()) != null) {
                  String[] id =  line.split(":::");
                  String[] lineNumbers = id[1].split(" ");
                  dic.put(id[0], lineNumbers);
            }
        }

        catch (Exception e){
            Toast.makeText(DictionaryActivity.this, "There seems to be a problem reading the dictionary file", Toast.LENGTH_LONG).show();
            finish();
        }
    }
}