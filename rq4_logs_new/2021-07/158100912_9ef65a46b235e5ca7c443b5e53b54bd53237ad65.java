import java.io.File;
import java.io.FileReader;
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;

public class Main {
    public static void main(String[] args) {
        if (args.length != 1) {
            final String className = new Object(){}.getClass().getEnclosingClass().getName();
            System.out.println("Usage:" + className + " <testdata.txt>");
            System.exit(1);
        }

        if ((new File(args[0])).exists() == false) {
            System.out.println(args[0] + " not found.");
            System.exit(1);
        }

        try {
            File file = new File(args[0]);

            BufferedReader br = new BufferedReader(new FileReader(file));
            String line;
            Solution sl = new Solution();

            while((line = br.readLine()) != null){
                String trimmed_line = line.trim();
                if (trimmed_line.equals(""))
                    continue;
                System.out.println(trimmed_line);
                sl.Main(trimmed_line);
            }

            br.close();
            sl = null;

        } catch(FileNotFoundException e) {
            System.out.println(e);
        } catch(IOException e) {
            System.out.println(e);
        }
    }
}