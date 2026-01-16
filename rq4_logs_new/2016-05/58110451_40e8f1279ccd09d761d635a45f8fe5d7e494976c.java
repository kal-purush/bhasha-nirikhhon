package com.example.administrator.smallvault.util;

import android.content.Context;
import android.graphics.Color;
import android.graphics.Typeface;
import android.text.SpannableString;
import android.text.style.ForegroundColorSpan;
import android.text.style.RelativeSizeSpan;

import com.example.administrator.smallvault.db.DBHelper;
import com.example.administrator.smallvault.db.entity.Zhichu;
import com.github.mikephil.charting.data.Entry;
import com.github.mikephil.charting.data.PieData;
import com.github.mikephil.charting.data.PieDataSet;
import com.github.mikephil.charting.utils.ColorTemplate;

import java.util.ArrayList;

/**
 * Created by Administrator on 2016/5/6.
 */
public class ChartUtil {

    private static volatile ChartUtil intance = null;

    private ChartUtil() {
    }

    public static ChartUtil getIntance() {
        if (intance == null) {
            synchronized (ChartUtil.class) {
                if (intance == null) {
                    intance = new ChartUtil();
                }
            }
        }
        return intance;
    }

    public SpannableString generateCenterText() {
        SpannableString s = new SpannableString("月结\n5月份");
        s.setSpan(new RelativeSizeSpan(2f), 0, 6, 0);
        s.setSpan(new ForegroundColorSpan(Color.GRAY), 6, s.length(), 0);
        return s;
    }

    public PieData generatePieData(Context context, int month) {
        ArrayList<Zhichu> list = DBHelper.getIntance(context).queryZhichu();
        if (list != null) {
            ArrayList<Entry> entries1 = new ArrayList<>();
            ArrayList<String> xVals = new ArrayList<>();

            xVals.add("餐饮");
            xVals.add("购物");
            xVals.add("娱乐");
            xVals.add("医疗");
            xVals.add("其他");

            int food = 0;
            for (Zhichu entity : list) {
                food = food + Integer.valueOf(entity.getFood());
            }
            entries1.add(new Entry((float) food, 0));

            int shopping = 0;
            for (Zhichu entity : list) {
                shopping = shopping + Integer.valueOf(entity.getShopping());
            }
            entries1.add(new Entry((float) shopping, 1));

            int play = 0;
            for (Zhichu entity : list) {
                play = play + Integer.valueOf(entity.getPlay());
            }
            entries1.add(new Entry((float) play, 2));

            int yiliao = 0;
            for (Zhichu entity : list) {
                yiliao = yiliao + Integer.valueOf(entity.getMedicine());
            }
            entries1.add(new Entry((float) yiliao, 3));

            int other = 0;
            for (Zhichu entity : list) {
                other = other + Integer.valueOf(entity.getOther());
            }
            entries1.add(new Entry((float) other, 4));


//            for (int i = 0; i < count; i++) {
//                xVals.add("entry" + (i + 1));
//                entries1.add(new Entry((float) 10, i));
//            }

            PieDataSet ds1 = new PieDataSet(entries1, "月结:" + month + "月份");
            ds1.setColors(ColorTemplate.VORDIPLOM_COLORS);
            ds1.setSliceSpace(2f);
            ds1.setValueTextColor(Color.WHITE);
            ds1.setValueTextSize(12f);

            PieData d = new PieData(xVals, ds1);
            d.setValueTypeface(Typeface.DEFAULT);

            return d;
        } else {
            return null;
        }
    }

}