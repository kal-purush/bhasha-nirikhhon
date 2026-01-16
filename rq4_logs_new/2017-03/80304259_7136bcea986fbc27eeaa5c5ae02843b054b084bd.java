package manager;

import exception.user.UserNotFoundException;
import model.ConnectionInfo;

import java.net.Socket;
import java.util.HashMap;
import java.util.Map;

public class ClientConnectionManager {

  Map<String, Socket> sockets;

  private static ClientConnectionManager instance;

  ClientConnectionManager() {
    sockets = new HashMap<>();
  }

  public static ClientConnectionManager getInstance() {
    if (instance == null) {
      instance = new ClientConnectionManager();
    }
    return instance;
  }

  /**
   * Sets up a connection with the given client.
   *
   * @param username non-null string of the user who wants to connect with the server.
   * @param connectionInfo non-null object containing host and port of the client.
   */
  public void setUpConnection(String username, ConnectionInfo connectionInfo) {

  }

  /**
   * Sends data to the given user on the connection already established.
   *
   * @param username non-null string of the user to send data to.
   * @param data non-null object containing the data to send.
   * @throws UserNotFoundException if the user did not already establish a connection.
   */
  public void sendData(String username, Object data) throws UserNotFoundException {

  }
}