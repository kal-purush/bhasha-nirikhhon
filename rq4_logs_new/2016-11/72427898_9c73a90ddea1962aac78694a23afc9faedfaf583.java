package com.hormiga6.androidapipractice;

import android.content.Context;
import android.content.pm.PackageInfo;
import android.content.pm.PackageManager;
import android.support.test.InstrumentationRegistry;
import android.support.test.filters.SmallTest;
import android.support.test.runner.AndroidJUnit4;
import android.util.Log;

import org.junit.Test;
import org.junit.runner.RunWith;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

/**
 * Created by kotaro.arimura on 2016/11/29.
 */

@RunWith(AndroidJUnit4.class)
@SmallTest
public class GetInformationTest {
    private static final String TAG = "GetInformationTest";
    @Test
    public void testPackageInfo() {
        Context context = InstrumentationRegistry.getContext();
        PackageInfo packageInfo = null;
        try {
            packageInfo = context.getPackageManager().getPackageInfo("com.hormiga6.androidapipractice", 0);
        } catch (PackageManager.NameNotFoundException e) {
            e.printStackTrace();
        }
        assertThat(packageInfo.versionName, is("1.0"));
        assertThat(context.getPackageName(), is("com.hormiga6.androidapipractice.test"));
    }

    @Test
    public void testGetDeviceInformation(){
        Log.d(TAG, "MODEL: " + android.os.Build.MODEL);
        Log.d(TAG, "PRODUCT: " + android.os.Build.PRODUCT);
        Log.d(TAG, "MANUFACTURER: " + android.os.Build.MANUFACTURER);
    }
}