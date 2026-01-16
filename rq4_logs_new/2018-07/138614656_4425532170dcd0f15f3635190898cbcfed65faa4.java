package com.andela.javadevelopers;

import android.content.Context;
import android.net.ConnectivityManager;
import android.net.NetworkInfo;

import com.andela.javadevelopers.util.Connectivity;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;


/**
 * The type Network connect test.
 */
public class NetworkConnectTest {
    /**
     * Context.
     */
    @Mock private Context context;
    /**
     * ConnectivityManager.
     */
    @Mock private ConnectivityManager cm;
    /**
     * NetworkInfo.
     */
    @Mock private NetworkInfo networkInfo;

    /**
     * The Mockito rule.
     */
    @Rule public MockitoRule mockitoRule = MockitoJUnit.rule();

    /**
     * Gets connectivity status returns true when active network info is not null.
     *
     * @throws Exception the exception
     */
    @Test
    public void getConnectivityStatusReturnsTrueWhenActiveNetworkInfoIsNotNull() throws Exception {
        Mockito.when(context.getSystemService(Context.CONNECTIVITY_SERVICE)).thenReturn(cm);
        Mockito.when(cm.getActiveNetworkInfo()).thenReturn(networkInfo);
        Mockito.when(networkInfo.isConnected()).thenReturn(true);

        Assert.assertTrue(Connectivity.isConnected(context));

        Mockito.verify(networkInfo).isConnected();
    }

    /**
     * Gets connectivity status returns false when active network info is null.
     *
     * @throws Exception the exception
     */
    @Test
    public void getConnectivityStatusReturnsFalseWhenActiveNetworkInfoIsNull() throws Exception {
        Mockito.when(context.getSystemService(Context.CONNECTIVITY_SERVICE)).thenReturn(cm);
        Mockito.when((cm.getActiveNetworkInfo())).thenReturn(null);

        Assert.assertNull(cm.getActiveNetworkInfo());
        Assert.assertFalse(Connectivity.isConnected(context));
    }

    /**
     * Gets connectivity status returns false when active network info is not null.
     *
     * @throws Exception the exception
     */
    @Test
    public void getConnectivityStatusReturnsFalseWhenActiveNetworkInfoIsNotNull() throws Exception {
        Mockito.when(context.getSystemService(Context.CONNECTIVITY_SERVICE)).thenReturn(cm);
        Mockito.when(cm.getActiveNetworkInfo()).thenReturn(networkInfo);
        Mockito.when(networkInfo.isConnected()).thenReturn(false);

        Assert.assertFalse(Connectivity.isConnected(context));
        Assert.assertEquals(networkInfo.isConnected(), false);
        Assert.assertFalse(networkInfo.isAvailable());
    }
}