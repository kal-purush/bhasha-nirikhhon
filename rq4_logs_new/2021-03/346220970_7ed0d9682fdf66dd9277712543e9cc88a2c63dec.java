import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;


import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.stream.IntStream;

public class NioFileApiTest
{
    private static final String HOME = System.getProperty("user.home");
    private static final String PLAY_WITH_NIO = "TempPlayGround";

    @Test
    void givenPathWhenCheckedThenConfirm() throws IOException
    {
        Path homePath = Paths.get(HOME);
        Assertions.assertTrue(Files.exists(homePath));

        Path playPath = Paths.get(HOME + "/"+PLAY_WITH_NIO);
        if (Files.exists(playPath))
            FileUtils.deleteFiles(playPath.toFile());
        Assertions.assertTrue(Files.notExists(playPath));

        Files.createDirectories(playPath);
        Assertions.assertTrue(Files.exists(playPath));

        IntStream.range(1,10).forEach(fileNumber ->
        {
            Path tempFile = Paths.get(playPath +"/temp" +fileNumber);
            Assertions.assertTrue(Files.notExists(tempFile));
            try
            {
                Files.createFile(tempFile);
            }
            catch(IOException e){System.out.println("Exception"+e);}
            Assertions.assertTrue(Files.exists(tempFile));
        });
        Files.list(playPath).filter(Files::isRegularFile).forEach(System.out::println);
        Files.newDirectoryStream(playPath).forEach(System.out::println);
        Files.newDirectoryStream(playPath,path ->path.toFile().isFile() &&path.toString().startsWith("temp"));

    }
    @Test
    void givenADirectoryWhenWatchListsAllActivities() throws IOException{
        Path dir = Paths.get(HOME+ "/"+PLAY_WITH_NIO);
        Files.list(dir).filter(Files::isRegularFile).forEach(System.out::println);
        new Java8WatchServiceExample(dir).processEvents();
    }
}