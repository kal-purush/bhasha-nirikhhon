import com.google.common.hash.HashCode;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import com.google.common.io.Files;

import java.io.*;
import java.nio.file.Path;
import java.net.URL;
import java.net.URLConnection;
import java.net.MalformedURLException;

public class analysis {

    public static void main(String[] args) {
        try
            {
                URL path = analysis.class.getResource("TestImage2.img");

                File f = new File(path.getFile());

                HashCode sha = Files.hash(f , Hashing.sha1());
                HashCode md5 = Files.hash(f , Hashing.md5());

                System.out.println("MD5: " + md5.toString());
                System.out.println("SHA-1: " + sha.toString());


            }
        catch (IOException ex) {
            System.err.println("Could not hash file " + ": " + ex);
            throw new RuntimeException(ex);
        }
    }
}