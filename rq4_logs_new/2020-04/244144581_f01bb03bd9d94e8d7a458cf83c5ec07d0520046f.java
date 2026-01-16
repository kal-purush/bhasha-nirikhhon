package gp.lcw.sd.by.g3p.extension.rest.News;

import org.springframework.context.annotation.Bean;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import javax.servlet.ServletOutputStream;
import javax.servlet.http.HttpServletResponse;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;

@RestController
@RequestMapping("/videos")
public class httpVideos  {
    private File file;
    public String path;
  /*="C:\\Users\\i\\Desktop\\创业资料\\视频\\YouTube.mp4"*/

    @RequestMapping("/video")
    public void getVideoData(HttpServletResponse response) {
        File file = new File(this.path);
        ServletOutputStream out = null;
        try {
            FileInputStream instream = new FileInputStream(file);
            byte[] b = new byte[1024];
            int length = 0;
            BufferedInputStream buf = new BufferedInputStream(instream);
            out = response.getOutputStream();
            BufferedOutputStream bot = new BufferedOutputStream(out);

            while ((length = buf.read(b)) != -1) {

                bot.write(b, 0, b.length);
            }
            buf.close();
            bot.close();
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    public void setPath(String getpath){
        this.path=getpath;
    }
}