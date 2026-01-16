package master;


import java.net.MalformedURLException;
import java.net.URL;

import org.junit.Assert;
import org.junit.Test;

import master.FileParser;

public class FileParserTest {
	
	@Test
	public void test() {
		FileParser testFileParser = new FileParser("/home/francium/new.txt");
		Assert.assertNotEquals(0, testFileParser.getRssLinksAL().size(), 0.1);	
		Boolean flag=false;
		String notURL=null;
		int notURLIndex=0;
		for(int i=0;i<testFileParser.getRssLinksAL().size();i++){
			try{
				URL isUrl = new URL(testFileParser.getRssLinksAL().get(i).toString());
			}catch(MalformedURLException e){
				flag=true;
				notURL=testFileParser.getRssLinksAL().get(i).toString();
				notURLIndex=i;
			}
		}
		if(flag) Assert.fail("Invalid URL links in source file.Like:"+testFileParser.getRssLinksAL().get(notURLIndex).toString());
		
	}

}