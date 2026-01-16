package crawling;

import crawling.file.InputFile;
import crawling.file.WriteFile;
import crawling.input.URLManager;
import crawling.nodes.NodeFormatter;
import crawling.nodes.NodeList;
import crawling.output.OutputFormat;
import crawling.output.WriteTree;
import org.junit.Test;
import static org.junit.Assert.*;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static crawling.output.OutputFormat.TXT;

/**
 * Created by midori on 2016/05/08.
 */
public class MainTest {

    @Test
    public void showTreeTest(){
        String filePath = "data/urlset/test.com.txt";
        List<String> urlList = new ArrayList<>();
        InputFile inFile = new InputFile();
        inFile.inputFile(filePath, urlList);

        // ファイルの1行目はhostURL
        URLManager urlManager = new URLManager(urlList.get(0));
        urlList.remove(0);

        // nodelist 作成
        NodeList nodeList = NodeList.generateNodeList(urlManager.getHost());
        NodeFormatter nodeFormat = new NodeFormatter(nodeList.getHost());
        nodeFormat.addUrlNodeList(urlList);

        WriteTree.writeTree(nodeList.getNodeList(), TXT, "test");

        String test = "data/txt/test.com.test.txt";
        String target = "data/txt/test.com.txt";

        Path testPath = Paths.get(test);
        Path targetPath = Paths.get(target);

        StringBuilder testS = new StringBuilder();
        StringBuilder targetS = new StringBuilder();

        String line = "";

        try(BufferedReader br = Files.newBufferedReader(testPath)){
            while(true){
                line = br.readLine();
                if(line == null){
                    break;
                }

                testS.append(line);
            }
        }catch (IOException e){
            e.printStackTrace();
        }

        try(BufferedReader br = Files.newBufferedReader(targetPath)){
            while(true){
                line = br.readLine();
                if(line == null){
                    break;
                }

                targetS.append(line);
            }
        }catch (IOException e){
            e.printStackTrace();
        }

        assertEquals(testS.toString(), targetS.toString());

    }

    @Test
    public void blankUrlSet() {
        Set<String> urlSet  = new HashSet<>();
        urlSet.add("gg/tt");
        urlSet.add("gg/tt/ge");
        urlSet.add("gg/tt/op");
        urlSet.add("");
        urlSet.add("");
        urlSet.add("");
        urlSet.add("");
        urlSet.add("");
        urlSet.add("gg/tt/ybojgabg");

        WriteFile wf = new WriteFile();
        wf.writeUrlSetFile(urlSet, "blank", "blank.com");

        InputFile in = new InputFile();
        List<String> urlList = new ArrayList<>();
        in.inputFile("data/urlset/blank.txt", urlList);

        for (String url : urlList) {
            assertNotEquals(url, "");
        }
    }
}