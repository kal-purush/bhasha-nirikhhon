package com.example.study;

import android.graphics.drawable.ColorDrawable;
import android.view.View;

import androidx.test.espresso.matcher.BoundedMatcher;

import org.hamcrest.Description;
import org.hamcrest.Matcher;

public class BackgroundColorMatcher {
    public static Matcher<View> withBackgroundColor(final int color) {
        return new BoundedMatcher<View, View>(View.class) {
            @Override
            public void describeTo(Description description) {
                description.appendText("with background color: ");
            }

            @Override
            public boolean matchesSafely(View view) {
                if (view.getBackground() instanceof ColorDrawable) {
                    ColorDrawable colorDrawable = (ColorDrawable) view.getBackground();
                    return color == colorDrawable.getColor();
                }
                return false;
            }
        };
    }
}