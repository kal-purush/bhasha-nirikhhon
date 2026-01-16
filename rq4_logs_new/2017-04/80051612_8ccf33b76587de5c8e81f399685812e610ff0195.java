package com.hannahburzynski.weatherapp;

import android.os.Bundle;
import org.junit.*;
import org.junit.Before;
import org.junit.Test;

/**
 * Created by Kyle Ratliff on 4/4/2017.
 */
public class AlertDialogFragmentTest {

    private AlertDialogFragment alertDialogFragment;
    private Bundle bundle;

    @Before
    public void initialize(){
        alertDialogFragment = new AlertDialogFragment();
        bundle = new Bundle();
    }

    @Test
    public void onCreateDialog() throws Exception {
        Assert.assertNotNull(bundle);
        alertDialogFragment.onCreateDialog(bundle);
    }

}