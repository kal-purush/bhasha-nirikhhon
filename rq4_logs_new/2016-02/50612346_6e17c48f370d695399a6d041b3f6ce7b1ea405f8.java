package com.superbrown.android.util;

import android.app.Activity;
import android.os.Bundle;

/**
 * Created with IntelliJ IDEA.
 * User: Mike
 * Date: 2/1/14
 * Time: 12:28 PM
 * To change this template use File | Settings | File Templates.
 */

import com.crittercism.app.Crittercism;

/**
 * Created by Mike on 2/1/14.
 */
public class DefectReportingActivity extends Activity
{

    public static final String MY_CRITTERCISM_APP_ID = "4f89a425b09315737a000069";

    @Override
    protected void onCreate(Bundle savedInstanceState)
    {
        super.onCreate(savedInstanceState);

        // defect reporting mechanism
        Crittercism.initialize(getApplicationContext(), MY_CRITTERCISM_APP_ID);
    }
}