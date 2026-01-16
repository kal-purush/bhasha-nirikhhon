package com.web.crawler.link.replacer.replacement.primitive;

import com.web.crawler.model.CrawledLink;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

public class AddExtensionTestSuite {

    //Given
    private String testAddress1 = "https://www.testCase.org/";
    private String testAddress2 = "https://www.testCase.org/domains/reserved/";
    private String testCase1 = "about/";
    private String testCase2 = "/domains";
    private String testCase3 = "/domains/root";
    private String testCase4 = "/domains/root/db";
    private String testCase5 = "/domains/idn-tables";
    private String testCase6 = "/time-zones";
    private String testCase7 = "http://www.icann.org/";
    private String testCase8 = "/_css/2015.1/screen.css";
    private String testCase9 = "about";
    private String testCase10 = "about.html";
    private String testCase11 = "/domains/root/db";
    private CrawledLink crawledLinkTestCase1;
    private CrawledLink crawledLinkTestCase2;
    private CrawledLink crawledLinkTestCase3;
    private CrawledLink crawledLinkTestCase4;
    private CrawledLink crawledLinkTestCase5;
    private CrawledLink crawledLinkTestCase6;
    private CrawledLink crawledLinkTestCase7;
    private CrawledLink crawledLinkTestCase8;
    private CrawledLink crawledLinkTestCase9;
    private CrawledLink crawledLinkTestCase10;
    private CrawledLink crawledLinkTestCase11;
    private AddExtension addExtension;

    @Before
    public void setUp() throws Exception {

        crawledLinkTestCase1 = new CrawledLink("", "href=\"", testCase1);
        crawledLinkTestCase2 = new CrawledLink("", "src=\"", testCase2);
        crawledLinkTestCase3 = new CrawledLink("", "href=\"", testCase3);
        crawledLinkTestCase4 = new CrawledLink("", "href=\"", testCase4);
        crawledLinkTestCase5 = new CrawledLink("", "href=\"", testCase5);
        crawledLinkTestCase6 = new CrawledLink("", "href=\"", testCase6);
        crawledLinkTestCase7 = new CrawledLink("", "href=\"", testCase7);
        crawledLinkTestCase8 = new CrawledLink("", "src=\"", testCase8);
        crawledLinkTestCase9 = new CrawledLink("", "href=\"", testCase9);
        crawledLinkTestCase10 = new CrawledLink("", "href=\"", testCase10);
        crawledLinkTestCase11 = new CrawledLink("", "href=\"", testCase11);

        addExtension = new AddExtension();
    }

    @Test
    public void shouldBuildLocalLinksWithExtension() {

        //When
        String result1 = addExtension.replace(crawledLinkTestCase1, testAddress1);
        String result2 = addExtension.replace(crawledLinkTestCase2, testAddress1);
        String result3 = addExtension.replace(crawledLinkTestCase3, testAddress1);
        String result4 = addExtension.replace(crawledLinkTestCase4, testAddress1);
        String result5 = addExtension.replace(crawledLinkTestCase5, testAddress1);
        String result6 = addExtension.replace(crawledLinkTestCase6, testAddress1);
        String result9 = addExtension.replace(crawledLinkTestCase9, testAddress1);
        String result11 = addExtension.replace(crawledLinkTestCase11, testAddress1);

        //Then
        assertEquals("href=\"about.html\"", result1);
        assertEquals("src=\"domains.html\"", result2);
        assertEquals("href=\"domains/root.html\"", result3);
        assertEquals("href=\"domains/root/db.html\"", result4);
        assertEquals("href=\"domains/idn-tables.html\"", result5);
        assertEquals("href=\"time-zones.html\"", result6);
        assertEquals("href=\"about.html\"", result9);
        assertEquals("href=\"domains/root/db.html\"", result11);

    }

    @Test
    public void shouldBuildLocalLinksWithReferenceToMainFolderAndWithExtension() {

        //When
        //When
        String result1 = addExtension.replace(crawledLinkTestCase1, testAddress2);
        String result2 = addExtension.replace(crawledLinkTestCase2, testAddress2);
        String result3 = addExtension.replace(crawledLinkTestCase3, testAddress2);
        String result4 = addExtension.replace(crawledLinkTestCase4, testAddress2);
        String result5 = addExtension.replace(crawledLinkTestCase5, testAddress2);
        String result6 = addExtension.replace(crawledLinkTestCase6, testAddress2);
        String result9 = addExtension.replace(crawledLinkTestCase9, testAddress2);
        String result11 = addExtension.replace(crawledLinkTestCase11, testAddress2);

        //Then
        assertEquals("href=\"../../about.html\"", result1);
        assertEquals("src=\"../../domains.html\"", result2);
        assertEquals("href=\"../../domains/root.html\"", result3);
        assertEquals("href=\"../../domains/root/db.html\"", result4);
        assertEquals("href=\"../../domains/idn-tables.html\"", result5);
        assertEquals("href=\"../../time-zones.html\"", result6);
        assertEquals("href=\"../../about.html\"", result9);
        assertEquals("href=\"../../domains/root/db.html\"", result11);
    }

    @Test
    public void shouldSupportOperation() {

        //When
        boolean result1 = addExtension.supports(testCase1);
        boolean result2 = addExtension.supports(testCase2);
        boolean result3 = addExtension.supports(testCase3);
        boolean result4 = addExtension.supports(testCase4);
        boolean result5 = addExtension.supports(testCase5);
        boolean result6 = addExtension.supports(testCase6);
        boolean result7 = addExtension.supports(testCase7);
        boolean result8 = addExtension.supports(testCase8);
        boolean result9 = addExtension.supports(testCase9);
        boolean result10 = addExtension.supports(testCase10);
        boolean result11 = addExtension.supports(testCase11);

        //Then
        assertTrue(result1);
        assertTrue(result2);
        assertTrue(result3);
        assertTrue(result4);
        assertTrue(result5);
        assertTrue(result6);
        assertFalse(result7);
        assertFalse(result8);
        assertTrue(result9);
        assertFalse(result10);
        assertTrue(result11);
    }
}