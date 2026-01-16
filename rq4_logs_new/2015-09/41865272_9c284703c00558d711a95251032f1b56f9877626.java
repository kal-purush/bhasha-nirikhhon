package de.cms.util;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.junit.After;
import org.junit.Before;

public class StreamingTestCase {

    protected JavaStreamingContext sc;
    protected SparkConf conf;
    protected TestServer testServer;

    @Before
    public void setupServer() throws Exception {
        conf = new SparkConf().setAppName("Log Counter").setMaster("local[2]");
        sc = new JavaStreamingContext(conf, Durations.milliseconds(100));
        testServer = new TestServer();
    }

    protected void startServers() throws InterruptedException {
        testServer.start();
        sc.start();
        // Spark startup wait
        Thread.sleep(500);
    }

    @After
    public void stopServer() throws InterruptedException {
        testServer.stopServer();
        sc.stop();
    }

}