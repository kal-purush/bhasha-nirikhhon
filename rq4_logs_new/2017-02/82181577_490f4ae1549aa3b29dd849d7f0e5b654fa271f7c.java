import java.io.File;
import java.io.FilenameFilter;
import java.io.PrintWriter;

/**
 * Created on 2017/2/24.
 */
public class FileNameSelector implements FilenameFilter {
    String extension = ".";

    public FileNameSelector(String fileExtensionNoDot) {
        extension += fileExtensionNoDot;
    }

    @Override
    public boolean accept(File dir, String name) {
        return name.endsWith(extension);
    }

    public static void main(String[] args) throws Exception {
        File directory = new File(".");
//        System.out.println(directory);

        /**
        // List all the files in directory
        File[] files = directory.listFiles();
        System.out.println("\n目录" + directory.getName() + "下的所有文件:");
        for (File file : files) {
            System.out.print(file.getName() + "\n");
        }
         */

        File txtFileList = new File("txtFileList.txt");
        // Create a file to save the txt file list
        PrintWriter output = new PrintWriter(txtFileList);

        // List all the txt files in directory
        File[] txtFiles = directory.listFiles(new FileNameSelector("txt"));
//        System.out.println("\n当前目录下的所有txt文件:");

        int number = 1; // 给txt文件列表前面添加序号
        int k;

        // Save the txt file list to txtFileList.txt
        for (File file : txtFiles) {
//            System.out.print(file.getName() + "\n");
            k = file.getName().indexOf("."); // 文件名后缀.所在的下标
            output.write(number + "." + file.getName().substring(0, k) + "\r\n"); // \r\n即为换行, substring(0, k)截取文件名后缀名之前的部分
            number++;
            output.flush(); // 把缓冲区内容压入文件
        }

        // Close file
        output.close();
    }
}