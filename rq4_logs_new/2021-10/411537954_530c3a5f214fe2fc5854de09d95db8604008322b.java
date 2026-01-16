package cooper.ui;

import java.io.IOException;

public class FileIoUi extends Ui {

    public static void showFileWriteError(IOException e) {
        show(LINE);
        show("Error writing to file ", false);
        show(e.getMessage(), true);
        show(LINE);
    }

    /**
     * Exception message to show file path error.
     **/
    public static void showFileCreationError(IOException e) {
        show(LINE);
        show("Error creating storage file: ", false);
        show(e.getMessage(), true);
        show(LINE);
    }

    public static void showInvalidFileDataError() {
        show(LINE);
        show("Invalid file data in storage files!");
        show(LINE);
    }

    public static void showPdfSuccessfullyGenerated() {
        show(LINE);
        show("The pdf file has been successfully generated!");
        show(LINE);
    }

    public static void showBackupFileSuccessfullyCreated() {
        show(LINE);
        show("The backup file has been successfully created!");
        show(LINE);
    }

    public static void showPostRequestError() {
        show(LINE);
        show("Error encountered when sending post request!");
        show(LINE);
    }

    public static void showConnectionError() {
        show(LINE);
        show("There was a problem with your connection.");
        show("Creating backup file now...");
        show(LINE);
    }

    public static void showMalformedUrlError() {
        show(LINE);
        show("The URL provided for LaTex compilation is incorrect!");
        show(LINE);
    }
}