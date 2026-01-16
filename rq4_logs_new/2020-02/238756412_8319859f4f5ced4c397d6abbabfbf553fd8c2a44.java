package camunication;

import data.DataPackage;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;

public class DataPackageTransmitter implements Transmitter {
    private DataInputStream incoming;
    private DataOutputStream outgoing;

    public DataPackageTransmitter(Socket connection) {
        try {
            incoming = new DataInputStream(connection.getInputStream());
            outgoing = new DataOutputStream(connection.getOutputStream());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void send(DataPackage dataPackage) {
        try {
            outgoing.write(dataPackage.getData());
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    @Override
    public DataPackage receive() {
        return null;
    }
}