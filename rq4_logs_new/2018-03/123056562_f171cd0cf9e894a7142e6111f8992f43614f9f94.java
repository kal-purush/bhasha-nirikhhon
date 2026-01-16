/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package Main;

/**
 *
 * @author zepingluo
 */

import java.io.*;
import java.nio.channels.FileChannel;

public class Copy
{
	public static void main(String[] args) throws IOException
	{
		File file = new File("/Users/zepingluo/downloads/calc.png");
		File target = new File("/Users/zepingluo/documents/calc1.png");
               // System.out.println(file.getPath());
               Copy copy = new Copy();
                copy.copyFile(file,target,false);
        }
        
        public void copyFile(File aSourceFile, File aTargetFile, boolean aAppend) {
    log("Copying files with streams.");
    ensureTargetDirectoryExists(aTargetFile.getParentFile());
    InputStream inStream = null;
    OutputStream outStream = null;
    try{
      try {
        byte[] bucket = new byte[32*1024];
        inStream = new BufferedInputStream(new FileInputStream(aSourceFile));
        outStream = new BufferedOutputStream(new FileOutputStream(aTargetFile, aAppend));
        int bytesRead = 0;
        while(bytesRead != -1){
          bytesRead = inStream.read(bucket); //-1, 0, or more
          if(bytesRead > 0){
            outStream.write(bucket, 0, bytesRead);
          }
        }
      }
      finally {
        if (inStream != null) inStream.close();
        if (outStream != null) outStream.close();
      }
    }
    catch (FileNotFoundException ex){
      log("File not found: " + ex);
    }
    catch (IOException ex){
      log(ex);
    }
  }
  private static void log(Object aThing){
    System.out.println(String.valueOf(aThing));
  }
  private void ensureTargetDirectoryExists(File aTargetDir){
    if(!aTargetDir.exists()){
      aTargetDir.mkdirs();
    }
  }
  
}
