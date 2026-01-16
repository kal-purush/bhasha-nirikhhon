package com.file.generate;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;

import org.apache.jmeter.config.Arguments;
import org.apache.jmeter.protocol.java.sampler.AbstractJavaSamplerClient;
import org.apache.jmeter.protocol.java.sampler.JavaSamplerContext;
import org.apache.jmeter.samplers.SampleResult;

public class Generate_common_text_file extends AbstractJavaSamplerClient {

	private static String label = "Generate text file";
	private String input_content;
	private String file_name;
	private String file_path;

	FileOutputStream _fos = null;
	OutputStreamWriter _osw = null;
	File file = null;

	public void setupTest(JavaSamplerContext context) {

		file_path = context.getParameter("file_path");
		file_name = context.getParameter("file_name");

		StringBuilder _builder = new StringBuilder();
		_builder.append(file_name);
		_builder.append(".txt");

		file = new File(file_path, _builder.toString());

		if (!file.exists()) {
			try {

				file.createNewFile();

			} catch (IOException e) {

				e.printStackTrace();

			}
		}

		try {
			_fos = new FileOutputStream(file, true);
			_osw = new OutputStreamWriter(_fos, "utf-8");
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (UnsupportedEncodingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	public void teardownTest(JavaSamplerContext arg0) {
		

		try {
			_osw.close();
			_fos.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public Arguments getDefaultParameters() {
		
		Arguments params = new Arguments();
		params.addArgument("input_content", "lng,lat,香港大学");
		params.addArgument("file_path", "/home/upsmart/");
		params.addArgument("file_name", "2013-09-25");

		return params;
	}

	@Override
	public SampleResult runTest(JavaSamplerContext context) {
		String file_name1 = context.getParameter("file_name");
		
		if(!file_name.equals(file_name1)){
			System.out.println("111111111111============11111111111111111");
			try {
				_osw.close();
				_fos.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
			StringBuilder _builder = new StringBuilder();
			_builder.append(file_name);
			_builder.append(".txt");

			file = new File(file_path, _builder.toString());

			if (!file.exists()) {
				try {
					file.createNewFile();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
			try {
				_fos = new FileOutputStream(file, true);
				_osw = new OutputStreamWriter(_fos, "utf-8");
			} catch (FileNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (UnsupportedEncodingException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}		
		
		input_content = context.getParameter("input_content");		
		try {			
				_osw.write(input_content);
				_osw.write("\r\n");
				_osw.flush();				
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}

		SampleResult sr;
		sr = new SampleResult();
		sr.setSampleLabel(label);
		try {
			sr.sampleStart(); // 记录程序执行时间，以及执行结果
			sr.setSuccessful(true);

		} catch (Throwable e) {
			sr.setSuccessful(false);
		} finally {
			sr.sampleEnd();
		}
		return sr;
	}

}