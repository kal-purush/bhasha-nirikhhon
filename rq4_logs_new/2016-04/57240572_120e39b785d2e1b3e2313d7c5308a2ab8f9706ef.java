import java.io.*;
import java.util.*;
import java.util.logging.Logger;

/**
 * Created by Nishanth Sivakumar and Sriram Balasubramanian on 3/5/16.
 */
public class FileUtil {

    private static final Logger LOGGER = Logger.getLogger(FileUtil.class.getName());
    private static String CLASS_NAME = FileUtil.class.getName();


    public static List<String> getFiles(String directoryPath){
        final String METHOD_NAME = "getFiles";

        LOGGER.entering(CLASS_NAME, METHOD_NAME);
        List<String> fileNames = new ArrayList<>();

        File dirNode = new File(directoryPath);
        if(dirNode.isDirectory()){
            for(File node: dirNode.listFiles()){
                if(node.isDirectory()){
                    fileNames.addAll(getFiles(node.getAbsolutePath()));
                }else{
                    fileNames.add(node.getAbsolutePath());
                }
            }
        }else{
            fileNames.add(dirNode.getAbsolutePath());
        }
        LOGGER.exiting(CLASS_NAME, METHOD_NAME);

        return fileNames;
    }

    public static Map<String,Integer> getFileTerms(String filepath){
        final String METHOD_NAME = "getFileTerms";
        LOGGER.entering(CLASS_NAME,METHOD_NAME);

        Map<String,Integer> termFreqMap = new HashMap<>();
        FileReader fileReader;
        try {
            fileReader = new FileReader(new File(filepath));
            BufferedReader bufferedReader = new BufferedReader(fileReader);
            while(bufferedReader.ready()){
                String line = bufferedReader.readLine();
                String[] tokens = line.split(" ");
                for(String token: tokens){
                    String[] processedWords = StringUtil.processWord(token);
                    for(String term: processedWords){
                        if(term.length() >= 3 && !term.toLowerCase().equals("the")){
                            if(termFreqMap.containsKey(term.toLowerCase())){
                                termFreqMap.put(term.toLowerCase(),termFreqMap.get(term.toLowerCase())+1);
                            }else{
                                termFreqMap.put(term.toLowerCase(),1);
                            }
                        }
                    }
                }
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
            LOGGER.warning(e.getMessage());
        } catch (IOException e) {
            e.printStackTrace();
            LOGGER.warning(e.getMessage());
        }

        LOGGER.exiting(CLASS_NAME, METHOD_NAME);
        return termFreqMap;
    }


}