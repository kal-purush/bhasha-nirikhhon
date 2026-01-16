package ControllerTest;

import Controller.ImageWeb;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.io.File;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations={"classpath*:spring-mvc.xml"})

public class ImageWebTest {
    @Autowired
    private ImageWeb image;
    @Test
    public void download() {
        String out= "C:\\Users\\Public\\Image\\Sample\\query1.jpg";
        image.download("data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAAYAAAAGCAYAAADgzO9IAAAAP0lEQVQIHWWMAQoAIAgDR/QJ/Ub//04+w7ZICBwcOg5FZi5iBB82AGzixEglJrd4TVK5XUJpskSTEvpdFzX9AB2pGziSQcvAAAAAAElFTkSuQmCC" );
        boolean exist = false;
        while(!exist){
            File file=new File(out);
            if(file.exists()){
                exist = true;
            }
        }
    }
}