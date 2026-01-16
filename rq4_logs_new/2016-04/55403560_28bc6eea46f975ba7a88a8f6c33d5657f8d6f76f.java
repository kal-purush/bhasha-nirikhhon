package com.cybulski.nazarko.exampleswipeitem.utils;

import android.content.Context;
import android.util.TypedValue;

/**
 * Created by nazarko on 4/5/16.
 */
public class SizeConverter {

  public  static int  Dp2px(int value ,Context context){
    return  (int) TypedValue.applyDimension(TypedValue.COMPLEX_UNIT_DIP, value, context.getResources().getDisplayMetrics());
  }
}