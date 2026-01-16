package Service.interfaces;

import data.interfaces.IGame;

/**
 * read and write Game Data from Files
 */
public interface IFileHandler {

    /**
     * read a Game Document
     *
     * @param path Path to the Document
     * @return IGame Object
     */
    IGame readGameFile(String path);

    /**
     * save a Game Object to a File
     *
     * @param game Game Object, that will be saved
     * @param path Path to the File
     * @return true if operation was successful
     */
    boolean saveGame(IGame game, String path);
}