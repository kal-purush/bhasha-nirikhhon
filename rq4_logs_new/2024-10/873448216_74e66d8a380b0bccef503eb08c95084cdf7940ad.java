package rdt;

import java.net.DatagramSocket;

public class SAW extends GBN
{
    @Override
    public Sender createSender(DatagramSocket socket)
    {
        return new SAWSender(socket);
    }

    @Override
    public Receiver createReceiver(DatagramSocket socket)
    {
        return new SAWReceiver(socket);
    }

}