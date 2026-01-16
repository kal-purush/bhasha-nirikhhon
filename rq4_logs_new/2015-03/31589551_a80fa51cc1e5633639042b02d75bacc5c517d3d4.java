package com.paypal.psix.fragments;

import android.content.Context;
import android.graphics.drawable.Drawable;
import android.preference.Preference;
import android.util.AttributeSet;
import android.view.View;
import android.widget.ImageView;
import android.widget.TextView;

import com.paypal.psix.R;

public class LargeIconPreference extends Preference {

    private Drawable icon;
    private CharSequence title;

    public LargeIconPreference(Context context, AttributeSet attrs) {
        this(context, attrs, 0);
    }

    public LargeIconPreference(Context context, AttributeSet attrs, int defStyle) {
        super(context, attrs, defStyle);
        setLayoutResource(R.layout.large_icon_preference);
    }

    @Override
    protected void onBindView(View view) {
        super.onBindView(view);
        ImageView imageView = (ImageView) view.findViewById(R.id.pref_icon);
        if (imageView != null && icon != null) {
            imageView.setImageDrawable(icon);
        }
        TextView prefTitle = (TextView) view.findViewById(R.id.pref_text);
        if (prefTitle != null && title != null) {
            prefTitle.setText(title);
        }
    }

    @Override
    public void setIcon(Drawable newIcon) {
        if ((newIcon == null && icon != null) || (newIcon != null && !newIcon.equals(icon))) {
            icon = newIcon;
            notifyChanged();
        }
    }

    @Override
    public Drawable getIcon() {
        return icon;
    }

    @Override
    public void setTitle(CharSequence title) {
        this.title = title;
    }

    @Override
    public CharSequence getTitle() {
        return title;
    }
}