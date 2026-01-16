package com.example.jondhc.yoyo;

import android.app.Activity;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.Collections;
import java.util.EnumMap;
import java.util.Map;

/**
 * Created by clem on 14/11/17.
 */

public class LocalData implements Serializable {
    private static File mFolder;
    Map<Levels, Boolean> statut_levels = Collections.synchronizedMap(new EnumMap<Levels, Boolean>(Levels.class));

        public LocalData(){
            for (Levels p : Levels.values()) {
                statut_levels.put(p, false);
            }
        }

        public void saveData(Activity pContext){
            if(mFolder == null){
                mFolder = pContext.getExternalFilesDir(null);
            }
            ObjectOutput out;
            try {
                File outFile = new File(mFolder, "yoyoSavedData.data");
                out = new ObjectOutputStream(new FileOutputStream(outFile));
                out.writeObject(this);
                out.close();
            } catch (Exception e) {e.printStackTrace();}
        }

        public static LocalData loadData(Activity pContext){
            if(mFolder == null){
                mFolder = pContext.getExternalFilesDir(null);
            }
            ObjectInput in;
            LocalData ss=null;
            try {
                File inFile = new File(mFolder, "yoyoSavedData.data");
                in = new ObjectInputStream(new FileInputStream(inFile));
                ss=(LocalData) in.readObject();
                in.close();
            } catch (Exception e) {e.printStackTrace();}
            return ss;
        }
    }