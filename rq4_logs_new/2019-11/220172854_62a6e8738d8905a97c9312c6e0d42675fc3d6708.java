package com.lyh.text;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

import com.liyinghua.StringUtils;

public class TestStringUtils {
	@Test
	public void testIsBlank() {
		//断言测试
		assertFalse(StringUtils.isBlank("aaa"));
		assertTrue(StringUtils.isBlank(" "));
	}
}