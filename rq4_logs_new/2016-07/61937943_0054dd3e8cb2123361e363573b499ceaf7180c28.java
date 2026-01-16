import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;


/**
 * Created by l on 7/12/16.
 */
public class HadoopBridge extends HttpServlet {
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        Configuration configuration = new Configuration();

        configuration.set("fs.defaultFS", "hdfs://localhost:9000");

        FileSystem fs = FileSystem.get(configuration);
       
        Path filePath = new Path("hdfs://localhost:9000/user/sandesh/secondResult.csv/part-r-00000");


        BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(filePath)));
        String line;

        while ((line = br.readLine()) != null) {

            System.out.println(line.trim());
        }
    }
    protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {

    }


}