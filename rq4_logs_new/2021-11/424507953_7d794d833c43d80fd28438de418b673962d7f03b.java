package com.hazelcast.maven.attribution;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugin.MojoFailureException;
import org.apache.maven.plugin.logging.Log;
import org.junit.Before;
import org.junit.Test;

public class AbstractAttributionMojoTest {

    private static final File TEST_SOURCE_JAR = new File("target/unittestdata.jar");

    AbstractAttributionMojo mojo;

    @Before
    public void setUp() {
        mojo = new AbstractAttributionMojo() {

            @Override
            protected Map<String, File> resolveSourceJars() {
                HashMap<String, File> srcJars = new HashMap<>();
                srcJars.put(gavKey("com.hazelcast.test", "attribution-test-artifact", "4.92.13"), TEST_SOURCE_JAR);
                return srcJars;
            }
        };
    }

    @Test
    public void test() throws MojoExecutionException, MojoFailureException, IOException {
        Log log = mock(Log.class);
        when(log.isErrorEnabled()).thenReturn(true);
        mojo.outputFile = new File("target/unittest/attribution.txt");
        mojo.serviceTimeoutMinutes = 5;
        mojo.execute();
        List<String> lines = Files.readAllLines(mojo.outputFile.toPath(), StandardCharsets.UTF_8);
        assertEquals(9, lines.size());
        assertEquals("com.hazelcast.test:attribution-test-artifact:4.92.13", lines.get(0));
        assertEquals("\t(C) Copyright 1997-2013, The True Robocop", lines.get(1));
        assertEquals("\t(c) 2000 Star Macrosystems", lines.get(2));
        assertEquals("\t(c) Can't believe noone is putting these copyright lines into their coffee.", lines.get(3));
        assertEquals("\t(c) { // what a nice condition", lines.get(4));
        assertEquals("\tCopyright 2011-2020 Darth Vader", lines.get(7));
        assertEquals("", lines.get(8));
    }

    @Test
    public void testExcludeAll() throws MojoExecutionException, MojoFailureException, IOException {
        Log log = mock(Log.class);
        when(log.isErrorEnabled()).thenReturn(true);
        mojo.exclusionPatterns = new ArrayList<String>();
        mojo.exclusionPatterns.add("^.*$");
        mojo.outputFile = new File("target/unittest/attribution-exclude-all.txt");
        mojo.serviceTimeoutMinutes = 5;
        mojo.execute();
        assertFalse(mojo.outputFile.exists());
    }

    @Test
    public void testExcludeSome() throws MojoExecutionException, MojoFailureException, IOException {
        Log log = mock(Log.class);
        when(log.isErrorEnabled()).thenReturn(true);
        mojo.exclusionPatterns = new ArrayList<String>();
        mojo.exclusionPatterns.add("Star");
        mojo.exclusionPatterns.add("nice condition");
        mojo.outputFile = new File("target/unittest/attribution-exclude-some.txt");
        mojo.serviceTimeoutMinutes = 5;
        mojo.execute();

        assertContentUsingExclusions();
    }

    private void assertContentUsingExclusions() throws IOException {
        List<String> lines = Files.readAllLines(mojo.outputFile.toPath(), StandardCharsets.UTF_8);
        assertEquals(7, lines.size());
        assertEquals("com.hazelcast.test:attribution-test-artifact:4.92.13", lines.get(0));
        assertEquals("\t(C) Copyright 1997-2013, The True Robocop", lines.get(1));
        assertEquals("\t(c) Can't believe noone is putting these copyright lines into their coffee.", lines.get(2));
        assertEquals("\tCopyright 2011-2020 Darth Vader", lines.get(5));
        assertEquals("", lines.get(6));
    }

    @Test
    public void testExcludeSomeUsingPatternFile() throws MojoExecutionException, MojoFailureException, IOException {
        Log log = mock(Log.class);
        when(log.isErrorEnabled()).thenReturn(true);
        mojo.exclusionPatternsFile = new File("target/test-exclusion-file");
        Files.write(mojo.exclusionPatternsFile.toPath(), "Star\nnice condition".getBytes(StandardCharsets.UTF_8));
        mojo.outputFile = new File("target/unittest/attribution-excludePatternsFile.txt");
        mojo.serviceTimeoutMinutes = 5;
        mojo.execute();

        assertContentUsingExclusions();
    }
}