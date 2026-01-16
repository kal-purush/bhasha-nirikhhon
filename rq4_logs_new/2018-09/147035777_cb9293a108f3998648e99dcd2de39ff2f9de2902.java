package com.example.vlad.androidapp;

import android.content.Context;
import android.content.SharedPreferences;
import android.os.Bundle;
import android.support.v7.app.AppCompatActivity;
import android.widget.ListView;
import android.widget.SimpleAdapter;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class UsersListActivity extends AppCompatActivity {

    ListView list;

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_users);
        displayData();
    }

    private void displayData() {
        list = findViewById(R.id.usersList);
        SharedPreferences sharedPref = getSharedPreferences("usersInfo", Context.MODE_PRIVATE);

        HashMap<String, String> usersMap = new HashMap<>();
        List<HashMap<String, String>> listItems = new ArrayList<>();
        SimpleAdapter adapter = new SimpleAdapter(this, listItems, R.layout.list_item,
                new String[]{"First Line", "Second Line"}, new int[]{R.id.full_name, R.id.phone});

        Set<String> user;
        for (int i = 1; i < sharedPref.getAll().size() + 1; i++) {
            user = sharedPref.getStringSet("user" + i, null);
            assert user != null;
            ArrayList<String> userInfo = new ArrayList<>(user);
            String fullName, surname = "", name = "", phone = "";

            for (int j = 0; j < userInfo.size(); j++) {
                String info = userInfo.get(j);
                if (info.contains("surname: ")) {
                    surname = info.split("surname: ")[1];
                } else if (info.contains("name: ")) {
                    name = info.split("name: ")[1];
                } else {
                    phone = info.split("phone: ")[1];
                }
            }
            fullName = surname + " " + name;

            usersMap.put(fullName, phone);
        }

        Iterator it = usersMap.entrySet().iterator();
        while (it.hasNext()) {
            HashMap<String, String> resultsMap = new HashMap<>();
            HashMap.Entry pair = (Map.Entry) it.next();
            resultsMap.put("First Line", pair.getKey().toString());
            resultsMap.put("Second Line", pair.getValue().toString());
            listItems.add(resultsMap);
        }
        list.setAdapter(adapter);

    }
}