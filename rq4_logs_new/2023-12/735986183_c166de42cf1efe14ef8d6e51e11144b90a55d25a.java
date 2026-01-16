import java.io.File;
import java.io.FilenameFilter;
public class FileFilter {
    public static void main(String[] args) {
        String folderPath = "C:\Users\bhala\OneDrive\Documents\Java-Day-5-AF";
        String extension = ".txt"; 
        File folder = new File(folderPath);
        File[] filteredFiles = getFilesWithExtension(folder, extension);
        System.out.println("Files with extension '" + extension + "' in folder '" + folderPath + "':");
        for (File file : filteredFiles) {
            System.out.println(file.getName());
        }
    }
    private static File[] getFilesWithExtension(File folder, final String extension) {
        File[] files = folder.listFiles(new FilenameFilter() {
            @Override
            public boolean accept(File dir, String name) {
                return name.endsWith(extension);
            }
        });
        return files != null ? files : new File[0];
    }
}