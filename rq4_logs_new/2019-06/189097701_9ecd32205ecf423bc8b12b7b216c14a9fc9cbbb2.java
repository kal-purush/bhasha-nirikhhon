package ru.fmtk.hlystov.examinationapp;

import org.springframework.stereotype.Service;

import java.io.*;
import java.util.Scanner;

@Service
public class TestConsole {
    private PipedOutputStream outToInput;
    private ByteArrayOutputStream outputBytes;
    private InputStream inputStream;
    private PrintStream printStream;

    public TestConsole() throws IOException {
        outputBytes = new ByteArrayOutputStream();
        printStream = new PrintStream(outputBytes);
        outToInput = new PipedOutputStream();
        inputStream = new PipedInputStream(outToInput);
    }

    public Scanner getCurrentScreenScanner() {
        printStream.flush();
        return new Scanner(new ByteArrayInputStream(outputBytes.toByteArray()));
    }

    public void printlnToStdin(String text) throws IOException {
        outToInput.write((text + '\n').getBytes());
        outToInput.flush();
    }

    public InputStream getInputStream() {
        return inputStream;
    }

    public PrintStream getPrintStream() {
        return printStream;
    }


}