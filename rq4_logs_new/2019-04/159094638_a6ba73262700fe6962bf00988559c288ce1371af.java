package com.hugovs.gls.input;

import com.hugovs.gls.receiver.AudioData;
import com.hugovs.gls.receiver.AudioInput;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.SocketException;

/**
 * An {@link AudioInput} implementation that receive audio from udp packets in a given port.
 *
 * @author Hugo Sartori
 */
public class UdpAudioInput implements AudioInput {

    private static final Logger log = Logger.getLogger(UdpAudioInput.class);

    private final DatagramSocket socket;
    private final byte[] receive;

    /**
     * Creates an {@link UdpAudioInput} instance.
     *
     * @param port: the port to listen to udp packets.
     * @param totalBufferSize: the buffer size in bytes to store received data.
     */
    public UdpAudioInput(int port, int totalBufferSize) {
        receive = new byte[totalBufferSize];
        try {
            socket = new DatagramSocket(port);
        } catch (SocketException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Read or wait for packets coming from the given port.
     *
     * @return  {@link AudioData} : representing the most recent packet received;
     *          {@code null}      : if a packet was received with error.
     */
    @Override
    public AudioData read() {

        final DatagramPacket packet = new DatagramPacket(receive, receive.length);

        try {
            socket.receive(packet);
            return AudioData.wrap(packet.getData());
        } catch (IOException e) {
            log.error("Failed to receive packet: ", e);
        }

        return null;

    }

}