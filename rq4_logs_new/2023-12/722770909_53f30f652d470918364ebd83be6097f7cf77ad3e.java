package model;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.List;

public class PythonScriptRunner {
    public void runPythonScript(String scriptPath, List<Object> args) throws IOException {
        try {
            // Build the command to run the Python script
            ProcessBuilder processBuilder = new ProcessBuilder("python3", scriptPath);

            // Append each argument to the command
            for (Object arg : args) {
                processBuilder.command().add(arg.toString());
            }

            processBuilder.redirectErrorStream(true);

            // Start and wait for the process to finish
            Process process = processBuilder.start();

            captureAndPrintOutput(process);

            process.waitFor();
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
    }

    private static void captureAndPrintOutput(Process process) {
        try {
            // Get the input stream of the process
            InputStream inputStream = process.getInputStream();

            // Create a BufferedReader to read the output
            BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));

            // Print each line of the output
            String line;
            while ((line = reader.readLine()) != null) {
                System.out.println(line);
            }

            // Close the BufferedReader
            reader.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}