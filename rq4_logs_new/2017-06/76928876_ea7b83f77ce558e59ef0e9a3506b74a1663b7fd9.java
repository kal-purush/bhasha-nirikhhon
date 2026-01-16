package com.itcode.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * Created by along on 17/6/3.
 * 客户端操作hdfs,
 */
public class HdfsClientDemo {
        Configuration configuration;
        FileSystem fileSystem;
    @Before
    public void init() throws Exception {
        configuration = new Configuration();
        configuration.set("fs.defaultFS","hdfs://mini1:9000");
        fileSystem = FileSystem.get(new URI("hdfs://mini1:9000"), configuration, "hadoop");
    }
    @Test
    public void testUpload() throws IOException {
        fileSystem.copyFromLocalFile(new Path("/Users/along/ATest/copyToHdfs"),new Path("/JavaAPIWithHdfs.txt"));
        fileSystem.close();
    }
}