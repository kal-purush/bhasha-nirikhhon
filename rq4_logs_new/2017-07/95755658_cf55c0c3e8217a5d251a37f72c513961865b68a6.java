package reading_writing_file;
import java.util.InputMismatchException;
import java.io.File;
/**
 * Created by zemoso on 4/7/17.
 */
public class Mainclass {
    public static void main(String[] args)throws ArrayIndexOutOfBoundsException,InputMismatchException{
        String fileRPath,fileWPath;
        String fileR=args[0];
        String fileW=args[1];
        fileRPath=new File(fileR).getAbsolutePath();// gives full path of the file from the root
        System.out.println(fileRPath);

        fileWPath= new File(fileW).getAbsolutePath();
        System.out.println(fileWPath);
        /*
         *this above function will convert file into string (readable format) and will also store the occurances of the different character
         *Then finally write this map into the text file as user give
         */
       ReadWrite.readFromFileAndWrite(fileRPath,fileWPath);
    }
}