package cooper.storage;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Scanner;

import cooper.ui.Ui;

public class Storage {

    private File storageFile;
    private ArrayList<String> historyInputs;

    public Storage() {
        historyInputs = new ArrayList<String>();
        String baseDir = System.getProperty("user.dir") + "/tmp";
        String fileName = "/storage.txt";
        Path folderDir = Paths.get(baseDir);
        Path fileDir = Paths.get(baseDir + fileName);
        try {
            if (Files.notExists(folderDir)) {
                Files.createDirectories(folderDir);
            }
        } catch (IOException e) {
            Ui.showInvalidFilePathError();
            // Wrong URI, no storage file is created
            storageFile = null;
            return;
        }
        // load storage file
        storageFile = new File(fileDir.toString());
    }

    public void loadStorage() {
        Ui.suppressOutput();

        try {
            Scanner sc = new Scanner(storageFile);
            while (sc.hasNextLine()) {
                Ui.showText(sc.nextLine());
            }
            sc.close();
        } catch (FileNotFoundException e) {
            Ui.unSuppressOutput();
            Ui.showNoStorage();
        }

        Ui.unSuppressOutput();
        return;
    }


    public void saveCommand(String input) {
        historyInputs.add(input);
    }

    public void saveStorage() {
        try {
            BufferedWriter writer = new BufferedWriter(new FileWriter(storageFile, true));
            for (String input : historyInputs) {
                writer.write(input);
                writer.newLine();
            }
            writer.close();
        } catch (IOException e) {
            Ui.showInvalidFilePathError();
        }
    }












}