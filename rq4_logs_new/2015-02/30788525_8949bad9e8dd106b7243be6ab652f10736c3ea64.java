package com.common.tools;

import java.io.IOException;

import junit.framework.TestCase;

public class ImageCompressorTest extends TestCase {
	ImageCompressor com=new ImageCompressor("C:\\Users\\Public\\Pictures\\Sample Pictures\\test.jpg");


	public void testResizeFix() {
		try {
			com.resizeFix(100, 100, "c:\\test.jpg");
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}