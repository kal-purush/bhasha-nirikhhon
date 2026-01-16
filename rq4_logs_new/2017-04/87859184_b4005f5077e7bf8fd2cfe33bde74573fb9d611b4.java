import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.*;
import java.time.LocalDateTime;

public class Main {
/*
Call Files.createFile to create an empty file that starts with a dot (".").
Call other methods on the Files class.
 Call the following methods in order and print out the return from the method.


Use the buffered writer and buffered reader.

     */
    public static void main(String[] args) throws IOException {
        String homeDir = System.getProperty("user.home");
        System.out.printf("homeDir: %s%n", homeDir);
        String fileSeparator = System.getProperty("file.separator");
        System.out.printf("fileSeparator: %s%n", fileSeparator);

        Path stealth = Paths.get(homeDir + fileSeparator + ".Stealth");
        Path b52 = Paths.get(homeDir + fileSeparator + ".Stealth" + fileSeparator + "b52");
     // if .Stealth folder does not exist at stealth path do ->
        if (!Files.exists(stealth)) {
            try {
                // try to create the directory, at stealth path.
                Files.createDirectory(stealth);
                // print a sys out saying created, show path.
                //Bunch of Souts about stealth
                System.out.printf("Created %s%n", stealth);

                //catch exception if we cant for some reason
            } catch (IOException e) {
                //sout saying we could not create it..
                System.out.printf("Could not create %s because of %s%n", stealth, e.getMessage());
            }
        } else {
            try {
                Files.createFile(b52);
            } catch (FileAlreadyExistsException e) {

            }
        }
        System.out.println("The file " +b52 + " already exists, here's info on it \n");
        System.out.println("exists -> " + Files.exists(stealth));
        System.out.println("isDirectory -> " + Files.isDirectory(stealth));
        System.out.println("isHidden -> " + Files.isHidden(stealth));
        System.out.println("isSameFile (as itself) -> " + Files.isSameFile(stealth,stealth));

        System.out.printf("info on b52 now \n ");
        System.out.println("Exists -> " + Files.exists(b52));
        System.out.println("isHidden ->" + Files.isHidden(b52));
        System.out.println("isExecutable -> " +Files.isExecutable(b52));
        System.out.println("the path is -> " + b52 + "\n");

        Charset charset = Charset.forName("US-ASCII");
        try (BufferedWriter writer = Files.newBufferedWriter(b52, charset, StandardOpenOption.APPEND)) {
            writer.write(LocalDateTime.now().toString());
            writer.newLine();
            writer.flush();
            System.out.println("wrote timestamp " +b52 + " successfully.. ");
        } catch (IOException e) {
            System.out.printf(b52 + " already exists");
        }
        try (BufferedReader reader = Files.newBufferedReader(b52)){
            String line = reader.readLine();
            StringBuilder sb = new StringBuilder();

            int counter = 1;
            while (line !=null){       //I know this runs forever
                sb.append(counter++ + ". " + line + "\n");
                line = reader.readLine();
            }
            System.out.print(sb);
            //slick ... tell me the total lines by decrementing counter.
            System.out.println("total lines: " + --counter);
        }
    }
}