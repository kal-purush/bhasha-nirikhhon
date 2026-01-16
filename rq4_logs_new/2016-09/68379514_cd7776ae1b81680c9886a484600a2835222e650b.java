package com.andres.wherearemyuser.database;

import android.content.Context;

import io.realm.Realm;
import io.realm.RealmConfiguration;


public class RealmManager {

    private static RealmManager realmManager;

    private RealmConfiguration realmConfig;

    public static RealmManager getInstance(Context context) {
        if (realmManager == null) {
            realmManager = new RealmManager(context.getApplicationContext());
        }
        return realmManager;
    }

    private RealmManager(Context context) {
        realmConfig = new RealmConfiguration.Builder(context)
                .deleteRealmIfMigrationNeeded()
                .build();
    }

    public Realm getRealm() {
        return Realm.getInstance(realmConfig);
    }
}