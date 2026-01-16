package com.odeproject;
/**
 * @class DataReader
 * @brief A class for reading data from a file
 */
class DataReader {
    /** The thread that reads data from the file. */
    private FileReaderThread readerThread;
    /** The name of the file to read from. */
    private String fileName;
    /**
     * @brief Constructor for DataReader class
     * @param filename The name of the file to read from
     * @details Creates a new FileReaderThread object to read data from the file on a new thread, and starts the thread.
     */
    public DataReader(String filename) {
        this.fileName = filename;
        readerThread = new FileReaderThread(filename);
        Thread thread = new Thread(readerThread);
        thread.start();
        try {
            thread.join();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
    /**
     * @brief Reads data from the file
     * @return The data read from the file as a string
     * Waits for the reader thread to finish reading the data from the file and returns the data.
     */
    public String readDataFromFile() {
        return readerThread.getData();
    }
}