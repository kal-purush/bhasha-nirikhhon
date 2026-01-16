/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package photoorganizer;

import com.drew.imaging.ImageMetadataReader;
import com.drew.imaging.ImageProcessingException;
import com.drew.metadata.Metadata;
import com.drew.metadata.exif.ExifSubIFDDirectory;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author eduardockerse
 */
public class PhotoOrganizer {

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {
        // TODO code application logic here
        processFolder(System.getProperty("user.dir"));
    }

    public static void processFolder(String path) {
        File folder = new File(path);
        File[] listOfFiles = folder.listFiles();
        for (File listOfFile : listOfFiles) {
            processFile(listOfFile);
        }

    }

    public static void processFile(File listOfFile) {
        if (listOfFile.isFile() && listOfFile.getAbsolutePath().endsWith("jpg")) {
            String filePath = listOfFile.getAbsolutePath();
            System.out.println("Showing metadata of file: " + filePath);
            File file = new File(filePath);
            try {
                Metadata metadata = ImageMetadataReader.readMetadata(file);

                ExifSubIFDDirectory directory = metadata.getFirstDirectoryOfType(ExifSubIFDDirectory.class);
                if (directory != null) {
                    Date date = directory.getDate(ExifSubIFDDirectory.TAG_DATETIME_ORIGINAL);
                    if (date != null) {
                        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("EEEE");
                        String day = simpleDateFormat.format(date);
                        simpleDateFormat = new SimpleDateFormat("MMMM");
                        String month = simpleDateFormat.format(date);
                        simpleDateFormat = new SimpleDateFormat("YYYY");
                        String year = simpleDateFormat.format(date);
                        moveFile(file, year, month);
                    }
                }
            } catch (ImageProcessingException | IOException ex) {
                Logger.getLogger(PhotoOrganizer.class.getName()).log(Level.SEVERE, null, ex);
            }
        } else if (listOfFile.isDirectory()) {
            processFolder(listOfFile.getAbsolutePath());
        }
    }

    public static void moveFile(File file, String yearFolder, String monthFolder) {
        try {
            File theDir = new File(System.getProperty("user.dir") + File.separatorChar + yearFolder);

            // if the directory does not exist, create it
            if (!theDir.exists()) {
                try {
                    theDir.mkdir();
                } catch (SecurityException se) {
                    //handle it
                }
            }
            theDir = new File(System.getProperty("user.dir") + File.separatorChar + yearFolder + File.separatorChar + monthFolder);
            if (!theDir.exists()) {
                try {
                    theDir.mkdir();
                } catch (SecurityException se) {
                    //handle it
                }
            }
            File newfile = new File(System.getProperty("user.dir") + File.separatorChar + yearFolder + File.separatorChar + monthFolder + File.separatorChar + file.getName());
            if (!newfile.exists()) {
                newfile.createNewFile();
            }
            InputStream inStream = new FileInputStream(file);
            OutputStream outStream = new FileOutputStream(newfile);

            byte[] buffer = new byte[1024];

            int length;
            //copy the file content in bytes 
            while ((length = inStream.read(buffer)) > 0) {
                outStream.write(buffer, 0, length);
            }

            inStream.close();
            outStream.close();

            //delete the original file
            //file.delete();    
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}