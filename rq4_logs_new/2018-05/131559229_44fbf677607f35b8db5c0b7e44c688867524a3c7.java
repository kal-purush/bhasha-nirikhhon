package com.systrangroup.unkown.app.handler;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Optional;

import org.apache.tomcat.util.codec.binary.Base64;
import org.junit.Test;

import lombok.Cleanup;
import lombok.extern.java.Log;

@Log
public class FileServiceTest {

	@Test
	public void testBinarySend() {
		File tgtFile = new File("/Users/min/Documents/multipart-test-workspace/KoenUnknownWordBatch.zip");
		Optional<String> binary = fileToString(tgtFile);
		if (binary.isPresent()) {
			stringToFile(binary.get());
		}
	}

	private Optional<String> fileToString(File tgtFile) {
		Optional<String> result = Optional.empty();
		try {
			@Cleanup FileInputStream fis = new FileInputStream(tgtFile);
			@Cleanup ByteArrayOutputStream baos = new ByteArrayOutputStream();

			int length = 0;
			byte[] buf = new byte[1024];
			while((length = fis.read(buf)) > 0) {
				baos.write(buf, 0, length);
			}
			
			byte[] fileArray = baos.toByteArray();
			result = Optional.of(new String(Base64.encodeBase64(fileArray)));
	        
		} catch (IOException e) {
			log.info(e.getMessage());
		}
		
		return result;
	}
	
	private void stringToFile(String res) {
		try {
			File resultFile = new File("/Users/min/Documents/multipart-test-workspace/result.zip");
	        @Cleanup FileOutputStream fos = new FileOutputStream(resultFile);
	        
	        byte[] fileData = Base64.decodeBase64(res);
	        fos.write(fileData);
		} catch(IOException e) {
			log.info(e.getMessage());
		}
	}
}