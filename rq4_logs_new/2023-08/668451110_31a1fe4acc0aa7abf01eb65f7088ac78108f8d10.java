package com.example.springboot;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.multipart.MultipartFile;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Objects;

/**
 * Controller class to handle file uploads in the Spring Boot application.
 */
@Controller
@SpringBootApplication
public class FileUploadController {

    /**
     * The upload folder for temporary storage of uploaded files.
     */
    private static final String UPLOAD_FOLDER = "/../../.tmp/";

    /**
     * Program entry.
     *
     * @param args is array of Strings
     *             waitForExit(); - Method for exit
     */
    public static void main(final String[] args) {
        SpringApplication.run(FileUploadController.class);
        waitForExit(); // Call the method to wait for 'q' to exit
    }

    /**
     * Handles the file upload process.
     *
     * @param file The uploaded file.
     * @return A redirect URL based on the upload result.
     */
    @PostMapping("/upload")
    public String handleFileUpload(
            @RequestParam("file") final MultipartFile file) {

        // Check for the presence of the file
        if (file.isEmpty()) {
            return "redirect:/index.html";
        }

        // Check if the file is a PDF
        if (!Objects.requireNonNull(
                        file.getContentType())
                .equalsIgnoreCase("application/pdf")) {

            return "redirect:/index.html?error=File is not a PDF";
        }

        // Determine the location of the class and create a file
        // in the \get-pdf\.tmp directory
        try {
            File classLocation =
                    new File(
                            FileUploadController.class
                                    .getProtectionDomain()
                                    .getCodeSource()
                                    .getLocation()
                                    .toURI());

            File tmpDirectory =
                    new File(
                            classLocation
                                    .getParentFile()
                                    .getParentFile(),
                            UPLOAD_FOLDER);

            if (!tmpDirectory.exists()) {
                //noinspection ResultOfMethodCallIgnored
                tmpDirectory.mkdir();
            }

            File destFile =
                    new File(
                            tmpDirectory,
                            Objects.requireNonNull(file.getOriginalFilename()));

            // Save the file to the \get-pdf\.tmp directory
            file.transferTo(destFile);

        } catch (IOException | URISyntaxException e) {
            e.fillInStackTrace();

            // Message for unsuccessful upload
            return "redirect:/index.html?error=Error saving the file";
        }

        // Message for successful upload
        return "redirect:/index.html?success=File upload completed";
    }

    /**
     * Waits for the user to press 'q' and then terminates the program.
     * <p>
     * This method keeps waiting for the user to press 'q' and then terminates
     * the program when 'q' is pressed.
     * It outputs a message to the console to prompt the user to press 'q'.
     * </p>
     */
    private static void waitForExit() {
        System.out.println("Press 'q' and Enter to exit.");
        try {
            while (System.in.read() != 'q') {
                System.out.println("reading...");
            }
        } catch (IOException e) {
            e.fillInStackTrace();
        }
        System.out.println("Exiting...");
        System.exit(0);
    }
}