package com.immortalidiot.studentapp;

import android.text.method.PasswordTransformationMethod;
import android.view.View;

public class NumericKeyboardTransformation extends PasswordTransformationMethod {
    @Override
    public CharSequence getTransformation(CharSequence source, View v) {
        return source;
    }
}