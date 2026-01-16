package hr.tvz.keepthechange.service;

import java.io.IOException;

/**
 * Contains SPI for working with file resources used in application.
 * Where and how files are stored depends on implementations.
 */
public interface FileManagerService {
    /**
     * Saves file given in a byte array on the given path to a file platform.
     *
     * @param pathString relative path where the file is stored
     * @param bytes      file bytes
     * @return {@code true} if a file was saved, {@code false} otherwise
     * @throws IOException if something goes wrong with saving the file
     */
    boolean saveFile(String pathString, byte[] bytes) throws IOException;
}