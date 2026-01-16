package ru.tohaman.rubicsguide.util;

import android.content.Context;
import android.content.res.TypedArray;

/**
 * Created by Toha on 12.12.2017. Сборник утилит
 */

public class Util {
    // возвращает массив целых числел, являющихся ссылками на ресурсы входящего массива (заданного тк же ссылкой)
    // т.е. на входе ссылка на <string-array name="ххх"> , а на выходе массив ссылок на элементы этого массива
    public static int[] getResID(int mId, Context mContext) {
        TypedArray tArray = mContext.getResources().obtainTypedArray(mId);
        int[] idx = null;
        if (tArray.length() != 0){
            int count = tArray.length();
            idx = new int[count];
            for (int i = 0; i < idx.length; i++) {
                idx[i] = tArray.getResourceId(i, 0);
            }
            tArray.recycle();
        }
        return idx;
    }

}