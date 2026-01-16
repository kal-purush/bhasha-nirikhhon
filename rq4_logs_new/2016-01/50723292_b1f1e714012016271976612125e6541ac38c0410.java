package com.example.tf.fiboapp;

import android.content.Intent;
import android.os.IBinder;
import android.os.Message;
import android.os.Messenger;
import android.os.RemoteException;
import android.support.test.InstrumentationRegistry;
import android.support.test.rule.ServiceTestRule;
import android.support.test.runner.AndroidJUnit4;
import android.test.suitebuilder.annotation.MediumTest;

import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.concurrent.TimeoutException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

/**
 * Created by TF on 1/30/2016.
 */
@MediumTest
@RunWith(AndroidJUnit4.class)
public class FibonacciServiceTest {
    @Rule
    public final ServiceTestRule mServiceRule = new ServiceTestRule();

    @Test
    public void testWithBoundService() throws TimeoutException {

        Intent serviceIntent =
                new Intent(InstrumentationRegistry.getTargetContext(), FibonacciService.class);

        IBinder binder = mServiceRule.bindService(serviceIntent);

        FibonacciService service =
                ((FibonacciService.LocalBinder) binder).getService();

        for(int i = 0; i < 200; i++) {
            service.FibonacciCalculation();
        }

        assertNotNull(service.getFibonacciSequence());

        assertEquals(service.getFibonacciSequence().get(18).toString(), new String("4181"));

        assertEquals(service.getFibonacciSequence().get(44).toString(), new String("1134903170"));

        int i = 46;
        assertEquals(service.getFibonacciSequence().size(), i);
    }

}