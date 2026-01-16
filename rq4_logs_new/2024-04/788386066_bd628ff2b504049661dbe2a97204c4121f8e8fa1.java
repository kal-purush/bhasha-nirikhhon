package master;

import model.Request;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;

public class RequestHandler extends Thread {

    private Socket socket;
    private List<Socket> workerConnections = new ArrayList<>();

    public Socket getSocket() {
        return socket;
    }

    public void setSocket(Socket socket) {
        this.socket = socket;
    }

    public List<Socket> getWorkerConnections() {
        return workerConnections;
    }

    public void setWorkerConnections(List<Socket> workerConnections) {
        this.workerConnections = workerConnections;
    }

    @Override
    public void run() {
        try (
                ObjectOutputStream output = new ObjectOutputStream(socket.getOutputStream());
                ObjectInputStream input = new ObjectInputStream(socket.getInputStream())
        ) {
            Request request = (Request) input.readObject();
            if (request.getAction() != null && request.getAction().equalsIgnoreCase("search")) {
                search(request);

            }
        } catch (IOException | ClassNotFoundException e) {
            System.out.println("Unexpected exception");
        }
    }

    private void search(Request request) {
        for(Socket workersocket: workerConnections) {
            try {
                ObjectOutputStream output = new ObjectOutputStream(workersocket.getOutputStream());
                ObjectInputStream input = new ObjectInputStream(workersocket.getInputStream());
                output.writeObject(request);
                output.flush();
                System.out.println("Sent search request with id " + request.getId());
            } catch(IOException io) {
                System.out.println("IO Exception while sending request");
            }
        }

        // accept reply from reducer
        // return reply to manager console
    }
}