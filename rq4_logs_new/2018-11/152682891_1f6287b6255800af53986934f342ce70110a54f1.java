import java.net.URL;
import java.io.*;
import java.util.Date;

class Urls extends Thread{
  private URL url;
  private String file;
  public Urls(URL url,String file) {
   this.url=url;
   this.file=file;
  }
      
  @Override
  public void run() {
   try{
             System.out.println(url);
             Date beginTime=new Date();
             download(url,file);
             Date endTime=new Date();
             System.out.println(file+"running time:"+(endTime.getTime()-beginTime.getTime())+"s");
         }catch(Exception ex){
             ex.printStackTrace();
         }
  }
  static void download( URL url, String file)
       throws IOException
       {
           try(InputStream input = url.openStream();
               OutputStream output = new FileOutputStream(file))
           {
           byte[] data = new byte[1024];
           int length;
           while((length=input.read(data))!=-1){
               output.write(data,0,length);
           }
           }
       }
}

public class threadDownloader{ 
 public static void main(String[] args)throws Exception {
        Urls downloader1=new Urls(new URL("https://www.pku.edu.cn"),"pku.htm"); 
        Urls downloader2=new Urls(new URL("https://www.baidu.com"),"baidu.htm"); 
        Urls downloader3=new Urls(new URL("https://www.sina.com.cn"),"sina.htm"); 
        Urls downloader4=new Urls(new URL("https://www.dstang.com"),"study.htm"); 
        downloader1.start();
        downloader2.start();
        downloader3.start();
        downloader4.start();
        
 }
}