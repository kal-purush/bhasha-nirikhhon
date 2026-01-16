package crawling.file;

import java.io.BufferedWriter;
import java.io.IOException;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Set;

/**
 * Created by midori on 2016/05/05.
 */
public class WriteFile {
    public void writeUrlSetFile(Set<String> urlSet, String fileName, String url){
        String ctrl = System.lineSeparator();
        Path filepath = Paths.get("data/urlset/" + fileName);
        try(BufferedWriter bw = Files.newBufferedWriter(filepath)){
            bw.write(url);
            bw.write(ctrl);
            for (String inUrl : urlSet) {
                bw.write(URLDecoder.decode(inUrl, StandardCharsets.UTF_8.name()));
                bw.write(ctrl);
            }

            System.out.println("書き込み完了");
            System.out.println("ファイル名 : " + fileName);
        }catch (IOException e){

        }
    }
}