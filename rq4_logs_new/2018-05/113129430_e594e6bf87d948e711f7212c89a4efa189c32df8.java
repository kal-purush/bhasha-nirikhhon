package org.springframework.context.support;

import org.junit.Test;

import static org.junit.Assert.assertTrue;

/**
 * Create by archerda on 2018/05/09.
 */
public class FileSystemXmlApplicationContextTests {

	private static final String PATH = "file:/Users/Archerda/Configuration/Java/spring-framework/spring-context/src/test/resources/org/springframework/context/support/";
	private static final String RESOURCE_CONTEXT = PATH + "FileSystemXmlApplicationContextTests-context.xml";

	@Test
	public void testSingleConfigLocation() {
		FileSystemXmlApplicationContext ctx = new FileSystemXmlApplicationContext(RESOURCE_CONTEXT);
		assertTrue(ctx.containsBean("testBean"));
		ctx.close();
	}
}