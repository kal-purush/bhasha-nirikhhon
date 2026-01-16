package unit.test.FileManager;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import log.Log;
import mx.fei.coilvicapp.logic.implementations.FileManager;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import org.junit.Test;

public class FileManagerTest {

    private final FileManager FILE_MANAGER_FOR_TEST = new FileManager();
    private File testFile;

    private File intializeValidFile() {
        File file = new File("files\\File.txt");
        return file;
    }

    private File initializeLargeFile() throws IOException {
        File largeFile = File.createTempFile("LargeFile", ".txt");

        try (FileWriter writer = new FileWriter(largeFile)) {
            StringBuilder data = new StringBuilder();
            for (int i = 0; i < 1000000; i++) {
                data.append("Lorem ipsum dolor sit amet, consectetur adipiscing elit. ");
            }
            writer.write(data.toString());
        } catch (IOException exception) {
            Log.getLogger(FileManagerTest.class).error(exception.getMessage(), exception);
        }

        return largeFile;
    }

    @Test
    public void testSaveAssignment() {
        int idCollaborativeProjectExample = 1234;
        String expectedDestionationPath = "files\\activities" + "\\" + idCollaborativeProjectExample+"\\certificate.pdf";
        String result = "u";
        File file = new File("files\\template\\certificate.pdf"); 
        
        FILE_MANAGER_FOR_TEST.setFile(file);
        FILE_MANAGER_FOR_TEST.setDestinationDirectory(idCollaborativeProjectExample);
        try {
            result = FILE_MANAGER_FOR_TEST.saveAssignment();
            FILE_MANAGER_FOR_TEST.undoSaveAssignment();
        } catch (IOException exception) {
            Log.getLogger(FileManagerTest.class).error(exception.getMessage(), exception);
        }
        System.out.println(expectedDestionationPath);
        System.out.println(result);
        assertEquals(expectedDestionationPath, result);
    }

    @Test
    public void isValidForSaveSucces() {
        boolean result = false;
        try {
            result = FILE_MANAGER_FOR_TEST.isValidFileForSave(intializeValidFile());
        } catch (IllegalArgumentException exception) {
            Log.getLogger(FileManagerTest.class).error(exception.getMessage(), exception);
        }
        assertTrue(result);
    }

    @Test
    public void isValidForSaveFailByTooLarge() {
        boolean result = false;
        try {
            result = FILE_MANAGER_FOR_TEST.isValidFileForSave(initializeLargeFile());
        } catch (IllegalArgumentException | IOException exception) {
            System.out.println(exception.getMessage());
            Log.getLogger(FileManagerTest.class).error(exception.getMessage(), exception);
        }
        assertTrue(result);
    }
    
    @Test
    public void isValidForSaveFailByNullFile() {
        boolean result = false;
        try {
            result = FILE_MANAGER_FOR_TEST.isValidFileForSave(null);
        } catch (IllegalArgumentException exception) {
            System.out.println(exception.getMessage());
            Log.getLogger(FileManagerTest.class).error(exception.getMessage(), exception);
        }
        assertTrue(result);
    }
    
    @Test
    public void isValidForSaveFailByExistenceFileAlready() {
        int idCollaborativeProjectExample = 1234;
        boolean result = false;
        File file = new File("files\\template\\certificate.pdf"); 
        
        FILE_MANAGER_FOR_TEST.setFile(file);
        FILE_MANAGER_FOR_TEST.setDestinationDirectory(idCollaborativeProjectExample);
        try {
            FILE_MANAGER_FOR_TEST.saveAssignment();
            result = FILE_MANAGER_FOR_TEST.isValidFileForSave(file);
        } catch (IllegalArgumentException | IOException exception) {
            System.out.println(exception.getMessage());
            FILE_MANAGER_FOR_TEST.undoSaveAssignment();
            Log.getLogger(FileManagerTest.class).error(exception.getMessage(), exception);
        }
        assertTrue(result);
    }

}