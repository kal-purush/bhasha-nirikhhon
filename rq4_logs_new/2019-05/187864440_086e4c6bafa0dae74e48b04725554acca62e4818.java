import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;


public class BuildData {
    public static void main(String[] args) throws Exception {


        File file = new File("/Users/tangke/Downloads/test.csv");
        FileInputStream fis = null;
        try {
            ReadFile readFile = new ReadFile();
            fis = new FileInputStream(file);
            int available = fis.available();
            System.out.println("文件大小" + available);

            int maxThreadNum = 2;
            // 线程粗略开始位置
            int i = available / maxThreadNum;
            for (int j = 0; j < maxThreadNum; j++) {
                // 计算精确开始位置
                long startNum = j == 0 ? 0 : readFile.getStartNum(file, i * j);
                long endNum = j + 1 < maxThreadNum ? readFile.getStartNum(file, i * (j + 1)) : -2;
                // 具体监听实现
                DealDataService listeners = new DealDataService("UTF-8");
                new ReadFileThread(listeners, startNum, endNum, file.getPath()).start();
            }


        } catch (IOException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}