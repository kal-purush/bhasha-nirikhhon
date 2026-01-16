package crawling.selector;

import crawling.file.InputFile;
import crawling.input.MakeUrlSet;
import crawling.node.Node;
import crawling.node.NodeFormatter;
import crawling.node.NodeList;
import crawling.output.WriteTree;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

import static crawling.output.OutputFormat.CONSOLE;
import static crawling.output.OutputFormat.TXT;

/**
 * Created by midori on 2016/05/05.
 */
public class ShowTree extends Selector {
    @Override
    public void select(){

        File dir = new File("data/urlset");
        File files[] = dir.listFiles();
        for (int i = 0; i < files.length; i++) {
            System.out.println(i + "." + files[i].getName());
        }
        System.out.print(">");

        String in = "";
        int fileNum = 0;
        try(BufferedReader br = new BufferedReader(new InputStreamReader(System.in))){
            in = br.readLine();
            //TODO:入力値判定
            fileNum = Integer.parseInt(in);
        }catch (IOException e){

        }

        // File input
        String filepath = files[fileNum].toString();
        InputFile inFile = new InputFile();
        List<String> urlList = new ArrayList<>();
        inFile.inputFile(filepath, urlList);

        URL url = null;
        try {
            url = new URL(urlList.get(0));
        }catch (MalformedURLException e){
            System.out.println("url error");
            return;
        }

        String host = url.getHost();
        urlList.remove(0);

        // nodelist 作成
        NodeList nodeList = NodeList.generateNodeList(host);
        Node hostNode = nodeList.getHost();
        NodeFormatter nodeFormat = new NodeFormatter(hostNode);
        nodeFormat.addUrlNodeList(urlList);

        WriteTree.writeTree(nodeList.getNodeList(), CONSOLE, "");
        WriteTree.writeTree(nodeList.getNodeList(), TXT, "data/tree2.txt");
    }
}