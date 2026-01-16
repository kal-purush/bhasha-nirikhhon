package com.andres.wherearemyuser.database;

import android.content.Context;

import com.andres.wherearemyuser.dataobjects.User;

import java.util.List;

import io.realm.Realm;

/**
 * Created by andresdavid on 17/09/16.
 */
public class DAOUser {

    public static void saveUsers(final List<User> userList, Context context) {
        RealmManager.getInstance(context).getRealm().executeTransactionAsync(
                new Realm.Transaction() {
                    @Override
                    public void execute(Realm realm) {
                        realm.insertOrUpdate(userList);
                    }
                }
        );
    }
    public static  List<User> getAllUser(Context context)
    {
        return  RealmManager.getInstance(context).getRealm().where(User.class).findAll();
    }
}